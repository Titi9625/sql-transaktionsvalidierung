"""Render actual dbt SQL and check batch isolation in an isolated SQLite database.

PostgreSQL UUID casts and reference-extraction syntax are adapted. This does
not replace a live dbt build against PostgreSQL.
"""

import csv
from decimal import Decimal
from pathlib import Path
import re
import sqlite3
import sys
from types import SimpleNamespace
import unittest

from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from python_import.batch_registry import prepare_pair
import validate_batch

PROJECT = ROOT/'dbt_validation_pipeline'
FIRST = 'bf63dc7e-97f9-492d-9c00-3b9193a04f41'
UPDATED = 'd043c60d-bc55-47ab-a786-a192194772e8'


def render(file,variables):
    def error(message):
        raise ValueError(message)
    env = Environment(undefined=StrictUndefined,extensions=['jinja2.ext.do'])
    env.globals.update(var=lambda name,default=None:variables.get(name,default),
        source=lambda name,table:table, ref=lambda name:name,
        config=lambda **options:'',modules=SimpleNamespace(re=re),
        exceptions=SimpleNamespace(raise_compiler_error=error))
    macro = (PROJECT/'macros/selected_batch_id.sql').read_text(encoding='utf-8')
    sql = env.from_string(macro+'\n'+file.read_text(encoding='utf-8')).render()
    return sql.replace('::uuid','').replace("substring(reference_text from '(txn_[0-9]+|pp_[0-9]+)')",'reference_id(reference_text)')


class BatchValidationTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.row_factory = sqlite3.Row
        def extract(value):
            match = re.search(r'(txn_[0-9]+|pp_[0-9]+)',value or '')
            return match.group() if match else None
        self.db.create_function('reference_id',1,extract)
        payment_schema = 'provider_transaction_id text,created text,currency text,amount integer,fee integer,net integer,status text,type text,reporting_category text,source text,imported_at text default CURRENT_TIMESTAMP'
        bank_schema = 'bank_transaction_id text,booking_date text,reference_text text,amount numeric,currency text,counterparty text,imported_at text default CURRENT_TIMESTAMP'
        self.db.execute('create table raw_payment_transactions ('+payment_schema+')')
        self.db.execute('create table raw_bank_transactions ('+bank_schema+')')
        self.db.execute('create table raw_payment_history (batch_id text,source_row_number integer,'+payment_schema+')')
        self.db.execute('create table raw_bank_history (batch_id text,source_row_number integer,'+bank_schema+')')
        self.db.execute('create table ingestion_batches (batch_id text primary key,status text,payment_row_count integer,bank_row_count integer)')
        for scenario,batch_id in [('01_initial',FIRST),('02_updated',UPDATED)]:
            folder = ROOT/'testdaten/batch_scenarios'/scenario
            pair = prepare_pair(folder/'payments.csv',folder/'bank.csv')
            self.db.execute('insert into ingestion_batches values (?,?,?,?)',(batch_id,'ready',len(pair.payment.rows),len(pair.bank.rows)))
            for dataset,table in [(pair.payment,'raw_payment_history'),(pair.bank,'raw_bank_history')]:
                columns = ['batch_id','source_row_number',*dataset.columns]
                rows = [(batch_id,number,*(str(value) if isinstance(value,Decimal) or column in {'created','booking_date'} else value
                    for column,value in ((column,row[column]) for column in dataset.columns)))
                    for number,row in enumerate(dataset.rows,1)]
                self.db.executemany('insert into '+table+' ('+','.join(columns)+') values ('+','.join('?' for _ in columns)+')',rows)
        # Original fixtures stay available alongside history, proving default
        # mode and selected mode read different inputs rather than mixing them.
        for filename,table in [('sample_stripe_transactions_large.csv','raw_payment_transactions'),('sample_bank_transactions_large.csv','raw_bank_transactions')]:
            with (ROOT/'testdaten'/filename).open(encoding='utf-8',newline='') as handle:
                reader = csv.DictReader(handle)
                columns = reader.fieldnames
                rows = []
                for row in reader:
                    values = []
                    for column in columns:
                        value = row[column]
                        if column in {'amount','fee','net'}:
                            value = None if value=='' else (float(Decimal(value)) if table=='raw_bank_transactions' else int(value))
                        values.append(value)
                    rows.append(values)
            self.db.executemany('insert into '+table+' ('+','.join(columns)+') values ('+','.join('?' for _ in columns)+')',rows)

    def build(self,variables):
        for folder,model in [('staging','dbt_stg_payment_transactions'),('staging','dbt_stg_bank_transactions'),
            ('validation','dbt_invalid_transactions'),('validation','dbt_valid_transactions'),('reports','dbt_validation_summary')]:
            self.db.execute(f'drop view if exists {model}')
            self.db.execute(f'create view {model} as '+render(PROJECT/'models'/folder/(model+'.sql'),variables))

    def test_initial_batch_has_expected_counts_without_cross_batch_duplicates(self):
        _,variables = validate_batch.scenario_expectations('01_initial')
        variables['batch_id'] = FIRST
        self.build(variables)
        summary = dict(self.db.execute('select * from dbt_validation_summary').fetchone())
        self.assertEqual([summary[key] for key in ['total_payment_records','total_bank_records','valid_records','invalid_payment_transactions','total_validation_errors']],[5,3,1,3,4])
        self.assertEqual(self.db.execute("select count(*) from dbt_invalid_transactions where error_code='duplicate_transaction_id'").fetchone()[0],2)
        for file in ['assert_selected_batch_ready.sql','assert_dbt_validation_summary_expected_values.sql','assert_batch_expected_error_counts.sql']:
            self.assertEqual(self.db.execute(render(PROJECT/'tests'/file,variables)).fetchall(),[])

    def test_updated_batch_uses_updated_status_and_still_only_two_duplicate_findings(self):
        _,variables = validate_batch.scenario_expectations('02_updated')
        variables['batch_id'] = UPDATED
        self.build(variables)
        self.assertEqual(self.db.execute('select transaction_id from dbt_valid_transactions order by transaction_id').fetchall()[1][0],'txn_102')
        summary = dict(self.db.execute('select * from dbt_validation_summary').fetchone())
        self.assertEqual([summary[key] for key in ['total_payment_records','total_bank_records','valid_records','invalid_payment_transactions','total_validation_errors']],[5,4,2,2,3])
        self.assertEqual(self.db.execute("select count(*) from dbt_invalid_transactions where error_code='duplicate_transaction_id'").fetchone()[0],2)
        for file in ['assert_selected_batch_ready.sql','assert_dbt_validation_summary_expected_values.sql','assert_batch_expected_error_counts.sql']:
            self.assertEqual(self.db.execute(render(PROJECT/'tests'/file,variables)).fetchall(),[])

    def test_original_mode_still_checks_original_fixture(self):
        self.build({})
        summary = dict(self.db.execute('select * from dbt_validation_summary').fetchone())
        self.assertEqual([summary[key] for key in ['total_payment_records','total_bank_records','valid_records','invalid_payment_transactions','total_validation_errors']],[50,40,20,25,46])
        self.assertEqual(self.db.execute(render(PROJECT/'tests/assert_dbt_validation_summary_expected_values.sql',{})).fetchall(),[])

    def test_unready_batch_is_excluded_and_readiness_test_fails(self):
        self.db.execute("update ingestion_batches set status='registered' where batch_id=?",(FIRST,))
        variables = {'batch_id':FIRST}
        self.build(variables)
        self.assertEqual(self.db.execute('select count(*) from dbt_stg_payment_transactions').fetchone()[0],0)
        self.assertEqual(len(self.db.execute(render(PROJECT/'tests/assert_selected_batch_ready.sql',variables)).fetchall()),1)

    def test_unknown_batch_cannot_silently_pass_empty_results(self):
        variables = {'batch_id':'00000000-0000-0000-0000-000000000000'}
        self.build(variables)
        self.assertEqual(len(self.db.execute(render(PROJECT/'tests/assert_selected_batch_ready.sql',variables)).fetchall()),1)

    def test_bad_uuid_is_rejected_before_sql_is_executed(self):
        for value in ["x' OR true --",'not-a-uuid',123]:
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'batch_id must be a UUID'):
                render(PROJECT/'models/staging/dbt_stg_payment_transactions.sql',{'batch_id':value})

    def test_wrong_expected_summary_and_error_distribution_fail(self):
        _,variables = validate_batch.scenario_expectations('01_initial')
        variables['batch_id'] = FIRST
        self.build(variables)
        variables['expected_summary']['total_validation_errors']=99
        self.assertEqual(len(self.db.execute(render(PROJECT/'tests/assert_dbt_validation_summary_expected_values.sql',variables)).fetchall()),1)
        variables['expected_error_counts']['pending_transaction']=2
        self.assertEqual(len(self.db.execute(render(PROJECT/'tests/assert_batch_expected_error_counts.sql',variables)).fetchall()),1)

    def test_expectations_are_tied_to_fixture_fingerprints(self):
        initial,first = validate_batch.scenario_expectations('01_initial')
        updated,second = validate_batch.scenario_expectations('02_updated')
        self.assertNotEqual(initial,updated)
        self.assertEqual(first['expected_summary']['valid_records'],1)
        self.assertEqual(second['expected_summary']['valid_records'],2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
