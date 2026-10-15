"""Build and test one ready batch in the separate batch_validation schema."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from uuid import UUID

from python_import.batch_registry import prepare_pair
from python_import.csv_contract import CsvFormatError
from python_import.result_history import build_proof, code_fingerprint, git_identity, save_results


ROOT = Path(__file__).resolve().parent
VALIDATION_LOCK_KEY = 736582104
SUMMARY_FIELDS = (
    'total_payment_records','total_bank_records','valid_records',
    'invalid_payment_transactions','total_validation_errors','duplicate_errors',
    'missing_value_errors','currency_errors','amount_mismatch_errors',
    'unmatched_bank_transactions','bank_reference_id_not_extractable_errors',
)
ERROR_CODES = (
    'duplicate_transaction_id','missing_amount','invalid_currency',
    'pending_transaction','refund_transaction','amount_mismatch','missing_bank_match',
    'unmatched_bank_transaction','bank_reference_id_not_extractable',
)


def scenario_expectations(scenario):
    """Tie exact expected counts to the fingerprints of the named fixture pair."""
    if scenario is None:
        return None, {}
    folder = ROOT / 'testdaten/batch_scenarios'
    pair = prepare_pair(folder/scenario/'payments.csv',folder/scenario/'bank.csv')
    expected = json.loads((folder/'expected_results.json').read_text(encoding='utf-8'))['scenarios'][scenario]['validation']
    summary = {field:expected[field] for field in SUMMARY_FIELDS}
    errors = {code:expected['error_counts'].get(code,0) for code in ERROR_CODES}
    if any(type(value) is not int or value < 0 for value in [*summary.values(),*errors.values()]):
        raise ValueError('Fixture expectations must be non-negative integer counts.')
    return pair.pair_fingerprint, {'expected_summary':summary,'expected_error_counts':errors}


def build_batch(batch_id, settings, variables):
    """Use the existing dbt installation and a temporary single-thread profile."""
    dbt = Path(sys.executable).parent / ('dbt.exe' if os.name == 'nt' else 'dbt')
    if not dbt.is_file():
        print('dbt executable missing; run with the repository .venv Python.',file=sys.stderr)
        return 1
    profile = {'bachelorarbeit_validation': {'target':'portfolio_batch','outputs':{
        'portfolio_batch': {'type':'postgres','host':settings['DB_HOST'],
            'port':int(settings['DB_PORT']),'dbname':settings['DB_NAME'],
            'user':settings['DB_USER'],'password':settings['DB_PASSWORD'],
            'schema':'batch_validation','threads':1,'connect_timeout':10}
    }}}
    with TemporaryDirectory(prefix='batch_validation_') as directory:
        (Path(directory)/'profiles.yml').write_text(json.dumps(profile),encoding='utf-8')
        command = [str(dbt),'build','--project-dir',str(ROOT/'dbt_validation_pipeline'),
            '--profiles-dir',directory,'--vars',json.dumps({'batch_id':str(batch_id),**variables})]
        try:
            subprocess.run(command,cwd=ROOT,check=True)
        except (subprocess.CalledProcessError,OSError):
            print('Batch validation stopped because dbt failed. No successful result is reported.',file=sys.stderr)
            return 1
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-id',required=True,type=UUID)
    parser.add_argument('--scenario',choices=['01_initial','02_updated'],
        help='Check exact counts against this known fixture; must match the batch fingerprints')
    parser.add_argument('--save-results',action='store_true',help='Save a permanent successful validation run')
    parser.add_argument('--init-history',action='store_true',help='Create result history tables if missing')
    args = parser.parse_args(argv)
    if args.init_history and not args.save_results:
        parser.error('--init-history requires --save-results')
    try:
        expected_fingerprint,variables = scenario_expectations(args.scenario)
    except (CsvFormatError,OSError,ValueError,KeyError,TypeError) as error:
        print(f'Could not prepare fixture expectations ({type(error).__name__}).',file=sys.stderr)
        return 1
    try:
        import psycopg2
        from dotenv import dotenv_values
    except ImportError:
        print('Use the repository .venv Python.',file=sys.stderr)
        return 1
    settings = dotenv_values(ROOT/'.env')
    for key in ['DB_HOST','DB_PORT','DB_NAME','DB_USER','DB_PASSWORD']:
        if not settings.get(key):
            print(f'Missing .env setting: {key}',file=sys.stderr)
            return 1
    if settings['DB_NAME'] != 'portfolio_validation':
        print('This step requires DB_NAME=portfolio_validation.',file=sys.stderr)
        return 1
    try:
        port = int(settings['DB_PORT'])
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        print('DB_PORT must be an integer from 1 to 65535.',file=sys.stderr)
        return 1
    connection = None
    try:
        connection = psycopg2.connect(host=settings['DB_HOST'],port=port,
            dbname=settings['DB_NAME'],user=settings['DB_USER'],
            password=settings['DB_PASSWORD'],connect_timeout=10)
        connection.set_session(readonly=True,autocommit=True)
        with connection.cursor() as cursor:
            # A session lock prevents two runs of this CLI from switching the
            # shared batch_validation views while another run reads its results.
            cursor.execute('SELECT pg_try_advisory_lock(%s)',(VALIDATION_LOCK_KEY,))
            if not cursor.fetchone()[0]:
                print('Another batch validation is running. Retry when it finishes.',file=sys.stderr)
                return 1
            cursor.execute('SELECT status,pair_fingerprint FROM public.ingestion_batches WHERE batch_id=%s',
                (str(args.batch_id),))
            metadata = cursor.fetchone()
            if metadata is None or metadata[0] != 'ready':
                print('The selected batch does not exist or is not ready.',file=sys.stderr)
                return 1
            if expected_fingerprint is not None and metadata[1] != expected_fingerprint:
                print('The named scenario does not match this batch. dbt was not started.',file=sys.stderr)
                return 1
        version = code_fingerprint(ROOT) if args.save_results else None
        result = build_batch(args.batch_id,settings,variables)
        if result:
            return result
        with connection.cursor() as cursor:
            cursor.execute('''SELECT total_payment_records,total_bank_records,valid_records,
                invalid_payment_transactions,total_validation_errors
                FROM batch_validation.dbt_validation_summary''')
            summary = cursor.fetchone()
        if summary is None:
            raise RuntimeError('Validation summary was not returned.')
        saved = None
        if args.save_results:
            if version != code_fingerprint(ROOT):
                raise ValueError('Validation code changed during the build; results were not saved.')
            proof = build_proof(ROOT,args.batch_id)
            git_commit,git_dirty = git_identity(ROOT)
            connection.set_session(readonly=False,autocommit=False,isolation_level='REPEATABLE READ')
            schema = (ROOT/'sql_views/07_create_validation_history.sql').read_text(encoding='utf-8') if args.init_history else None
            saved = save_results(connection,args.batch_id,version,proof,git_commit,git_dirty,schema)
            summary = saved.summary
    except Exception as error:
        print(f'Batch validation failed ({type(error).__name__}).',file=sys.stderr)
        return 1
    finally:
        if connection is not None:
            connection.close()
    print(f'Batch validation completed: {args.batch_id}')
    for label,value in zip(['Payment rows','Bank rows','Valid payments','Flagged payment IDs','Error rows'],summary):
        print(f'{label}: {value}')
    if saved is not None:
        print(f'Saved validation run: {saved.validation_run_id}')
        print('Summary, valid payments, findings and daily report are saved permanently.')
    else:
        print('Results are current views in batch_validation. Add --save-results to preserve this run.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
