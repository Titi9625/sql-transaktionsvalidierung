"""Save successful validation outputs atomically, retaining duplicate findings."""

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import subprocess
from uuid import uuid4


SUMMARY_COLUMNS = ('validation_date','total_payment_records','total_bank_records',
    'valid_records','invalid_payment_transactions','total_validation_errors',
    'duplicate_errors','missing_value_errors','currency_errors','amount_mismatch_errors',
    'unmatched_bank_transactions','bank_reference_id_not_extractable_errors')
VALID_COLUMNS = ('transaction_id','transaction_date','amount_gross','fee_amount',
    'amount_net','bank_amount','currency','status','validation_status')
FINDING_COLUMNS = ('transaction_id','record_source','error_code','error_description',
    'error_field','detected_at')
REPORT_COLUMNS = ('reporting_key','payment_date','source_system','payment_row_count',
    'distinct_payment_count','valid_payment_count','flagged_payment_count',
    'payment_error_row_count','duplicate_payment_id_count','validation_pass_rate_pct',
    'valid_net_amount_eur')


@dataclass
class SavedValidationRun:
    validation_run_id: str
    summary: tuple


def code_fingerprint(root):
    """Hash selected source/config files, excluding credentials and generated targets."""
    root = Path(root)
    project = root/'dbt_validation_pipeline'
    files = [project/'dbt_project.yml',root/'validate_batch.py',
        root/'python_import/result_history.py',root/'sql_views/07_create_validation_history.sql']
    for folder,patterns in [('models',['*.sql','*.yml']),('macros',['*.sql']),('tests',['*.sql'])]:
        for pattern in patterns:
            files.extend((project/folder).rglob(pattern))
    digest = hashlib.sha256()
    for file in sorted(set(files)):
        digest.update(file.relative_to(root).as_posix().encode('utf-8')+b'\0')
        digest.update(file.read_bytes()+b'\0')
    return digest.hexdigest()


def git_identity(root):
    env = {**os.environ,'GIT_OPTIONAL_LOCKS':'0'}
    try:
        commit = subprocess.run(['git','-C',str(root),'rev-parse','HEAD'],
            capture_output=True,text=True,check=True,env=env).stdout.strip()
        status = subprocess.run(['git','-C',str(root),'status','--porcelain'],
            capture_output=True,text=True,check=True,env=env).stdout
        return commit,bool(status.strip())
    except (OSError,subprocess.CalledProcessError):
        return None,None


def build_proof(root,batch_id):
    """Require successful model/test results tied to this invocation's selected batch."""
    data = json.loads((Path(root)/'dbt_validation_pipeline/target/run_results.json').read_text(encoding='utf-8'))
    variables = data.get('args',{}).get('vars',{})
    if isinstance(variables,str):
        variables = json.loads(variables)
    if str(variables.get('batch_id')) != str(batch_id):
        raise ValueError('The dbt artifact belongs to another batch.')
    results = data['results']
    models = [result for result in results if result['unique_id'].startswith('model.')]
    tests = [result for result in results if result['unique_id'].startswith('test.')]
    if len(models) != 6 or not tests or any(result['status']!='success' for result in models) or any(result['status']!='pass' for result in tests):
        raise ValueError('Not all required dbt models and tests succeeded.')
    return {'invocation_id':data['metadata']['invocation_id'],
        'dbt_version':data['metadata']['dbt_version'],'models':len(models),
        'tests':len(tests),'variables':variables}


def _copy_results(cursor,run_id):
    for table,view,columns in [
        ('validation_summaries','dbt_validation_summary',SUMMARY_COLUMNS),
        ('validation_valid_payments','dbt_valid_transactions',VALID_COLUMNS),
        ('validation_daily_reports','mart_daily_reconciliation',REPORT_COLUMNS),
    ]:
        names = ','.join(columns)
        cursor.execute(f'INSERT INTO public.{table} (validation_run_id,{names}) '
            f'SELECT %s,{names} FROM batch_validation.{view}',(run_id,))
    names = ','.join(FINDING_COLUMNS)
    cursor.execute(f'''INSERT INTO public.validation_findings
        (validation_run_id,finding_number,{names})
        SELECT %s,ROW_NUMBER() OVER (
            ORDER BY transaction_id,record_source,error_code,error_field,error_description
        ),{names} FROM batch_validation.dbt_invalid_transactions''',(run_id,))


def save_results(connection,batch_id,version,proof,git_commit=None,git_dirty=None,schema_sql=None):
    """Own one write transaction; caller holds the validation session advisory lock.

    View locks and UUID checks guard against switching the selected batch during
    capture. Use a fresh transaction, preferably REPEATABLE READ. Copies and
    their run record commit together. No previous saved row is updated/deleted.
    """
    run_id = str(uuid4())
    with connection:
        with connection.cursor() as cursor:
            if schema_sql is not None:
                cursor.execute(schema_sql)
            cursor.execute('''LOCK TABLE batch_validation.dbt_stg_payment_transactions,
                batch_validation.dbt_stg_bank_transactions,
                batch_validation.dbt_invalid_transactions,
                batch_validation.dbt_valid_transactions,
                batch_validation.dbt_validation_summary,
                batch_validation.mart_daily_reconciliation IN ACCESS SHARE MODE''')
            for view in ['dbt_stg_payment_transactions','dbt_stg_bank_transactions']:
                cursor.execute('SELECT pg_get_viewdef(%s::regclass,true)',('batch_validation.'+view,))
                definition = cursor.fetchone()
                if definition is None or str(batch_id) not in definition[0]:
                    raise ValueError('Staging views do not point to the selected batch.')
            cursor.execute('''SELECT payment_row_count,bank_row_count FROM public.ingestion_batches
                WHERE batch_id=%s AND status='ready' FOR SHARE''',(str(batch_id),))
            source_counts = cursor.fetchone()
            if source_counts is None:
                raise ValueError('The selected batch is not ready.')
            cursor.execute('''INSERT INTO public.validation_runs (
                validation_run_id,batch_id,code_version,git_commit,git_dirty,
                dbt_invocation_id,dbt_version,dbt_models_built,dbt_tests_passed,variables
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (run_id,str(batch_id),version,git_commit,git_dirty,proof['invocation_id'],
                 proof['dbt_version'],proof['models'],proof['tests'],json.dumps(proof['variables'])))
            _copy_results(cursor,run_id)
            cursor.execute('''SELECT total_payment_records,total_bank_records,valid_records,
                invalid_payment_transactions,total_validation_errors
                FROM public.validation_summaries WHERE validation_run_id=%s''',(run_id,))
            summaries = cursor.fetchall()
            if len(summaries)!=1 or tuple(summaries[0][:2])!=tuple(source_counts):
                raise ValueError('Saved summary does not match the source batch counts.')
            summary = tuple(summaries[0])
            cursor.execute('''SELECT
                (SELECT count(*) FROM public.validation_valid_payments WHERE validation_run_id=%s),
                (SELECT count(*) FROM public.validation_findings WHERE validation_run_id=%s)
            ''',(run_id,run_id))
            if tuple(cursor.fetchone()) != (summary[2],summary[4]):
                raise ValueError('Saved detail counts disagree with the saved summary.')
    return SavedValidationRun(run_id,summary)
