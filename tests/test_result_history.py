"""Snapshot preservation and atomic writes in isolated SQLite, plus dbt proof guards.

The adapter omits PostgreSQL view locks and reads SQLite view definitions instead.
Live PostgreSQL snapshot/schema compatibility is checked by the batch pipeline.
"""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
from pathlib import Path
import re
import sqlite3
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import test_batch_validation as validation_tests
import validate_batch
import run_batch_pipeline
from python_import.result_history import build_proof, code_fingerprint, save_results
import python_import.result_history as history

FIRST,UPDATED,PROJECT = validation_tests.FIRST,validation_tests.UPDATED,validation_tests.PROJECT
render = validation_tests.render


class CursorAdapter:
    def __init__(self,connection):
        self.connection = connection
        self.cursor = connection.cursor()
        self.override = None
    def __enter__(self):
        return self
    def __exit__(self,*args):
        self.cursor.close()
    def execute(self,query,parameters=()):
        self.override = None
        if query.strip().startswith('LOCK TABLE'):
            return
        if 'pg_get_viewdef' in query:
            row = self.connection.execute('select sql from sqlite_master where type=\'view\' and name=?',(parameters[0].split('.')[-1],)).fetchone()
            self.override = [tuple(row)] if row else []
            return
        sql = query.replace('public.','').replace('batch_validation.','').replace('%s','?').replace('FOR SHARE','')
        self.cursor.execute(sql,parameters)
    def fetchone(self):
        return self.override.pop(0) if self.override else self.cursor.fetchone()
    def fetchall(self):
        return self.override if self.override is not None else self.cursor.fetchall()


class ConnectionAdapter:
    def __init__(self,connection):
        self.connection = connection
    def cursor(self):
        return CursorAdapter(self.connection)
    def __enter__(self):
        return self
    def __exit__(self,kind,error,traceback):
        self.connection.rollback() if kind else self.connection.commit()


class ResultHistoryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = validation_tests.BatchValidationTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.db = self.fixture.db
        self.db.commit()
        self.db.execute('PRAGMA foreign_keys=ON')
        schema = (ROOT/'sql_views/07_create_validation_history.sql').read_text()
        schema = schema.replace('public.','').replace('clock_timestamp()','CURRENT_TIMESTAMP')
        schema = schema.replace(" CHECK (code_version ~ '^[0-9a-f]{64}$')",'')
        self.db.executescript(schema)
        self.adapter = ConnectionAdapter(self.db)

    def select_batch(self,batch_id,scenario):
        _,variables = validate_batch.scenario_expectations(scenario)
        variables['batch_id'] = batch_id
        self.fixture.build(variables)
        self.db.execute('drop view if exists mart_daily_reconciliation')
        sql = render(PROJECT/'models/reports/mart_daily_reconciliation.sql',variables)
        sql = re.sub(r'::(?:bigint|numeric|text)\b','',sql)
        self.db.execute('create view mart_daily_reconciliation as '+sql)
        self.db.commit()
        return {'invocation_id':'test-invocation','dbt_version':'1.12.2',
            'models':6,'tests':30,'variables':variables}

    def save(self,batch_id,scenario):
        proof = self.select_batch(batch_id,scenario)
        return save_results(self.adapter,batch_id,'a'*64,proof,'b'*40,True)

    def test_saved_results_survive_selecting_another_batch(self):
        first = self.save(FIRST,'01_initial')
        updated = self.save(UPDATED,'02_updated')
        self.assertEqual(first.summary,(5,3,1,3,4))
        self.assertEqual(updated.summary,(5,4,2,2,3))
        rows = self.db.execute('select valid_records,total_validation_errors from validation_summaries order by rowid').fetchall()
        self.assertEqual([tuple(row) for row in rows],[(1,4),(2,3)])

    def test_duplicate_findings_are_not_collapsed(self):
        first = self.save(FIRST,'01_initial')
        count = self.db.execute("select count(*) from validation_findings where validation_run_id=? and error_code='duplicate_transaction_id'",(first.validation_run_id,)).fetchone()[0]
        self.assertEqual(count,2)

    def test_repeat_validation_creates_another_run_without_rewriting_first(self):
        first = self.save(FIRST,'01_initial')
        before = [tuple(row) for row in self.db.execute('select * from validation_summaries where validation_run_id=?',(first.validation_run_id,))]
        other = self.save(FIRST,'01_initial')
        self.assertNotEqual(first.validation_run_id,other.validation_run_id)
        self.assertEqual(self.db.execute('select count(*) from validation_runs').fetchone()[0],2)
        self.assertEqual([tuple(row) for row in self.db.execute('select * from validation_summaries where validation_run_id=?',(first.validation_run_id,))],before)

    def test_failure_after_copy_rolls_back_all_snapshot_tables(self):
        proof = self.select_batch(FIRST,'01_initial')
        original = history._copy_results
        def copy_then_fail(cursor,run_id):
            original(cursor,run_id)
            raise ValueError('Deliberate failure after snapshot copying')
        with patch.object(history,'_copy_results',side_effect=copy_then_fail):
            with self.assertRaises(ValueError):
                save_results(self.adapter,FIRST,'a'*64,proof)
        for table in ['validation_runs','validation_summaries','validation_valid_payments','validation_findings','validation_daily_reports']:
            self.assertEqual(self.db.execute('select count(*) from '+table).fetchone()[0],0)
        self.assertEqual(self.db.execute('select count(*) from raw_payment_history').fetchone()[0],10)

    def test_wrong_selected_views_cannot_be_saved_under_another_batch(self):
        proof = self.select_batch(UPDATED,'02_updated')
        with self.assertRaisesRegex(ValueError,'Staging views'):
            save_results(self.adapter,FIRST,'a'*64,proof)
        self.assertEqual(self.db.execute('select count(*) from validation_runs').fetchone()[0],0)

    def test_inconsistent_source_counts_prevent_snapshot_publication(self):
        proof = self.select_batch(FIRST,'01_initial')
        self.db.execute('update ingestion_batches set payment_row_count=999 where batch_id=?',(FIRST,))
        self.db.commit()
        with self.assertRaisesRegex(ValueError,'source batch counts'):
            save_results(self.adapter,FIRST,'a'*64,proof)
        self.assertEqual(self.db.execute('select count(*) from validation_runs').fetchone()[0],0)

    def test_exact_valid_amounts_and_daily_metrics_are_saved(self):
        first = self.save(FIRST,'01_initial')
        amount = self.db.execute('select amount_net,bank_amount from validation_valid_payments where validation_run_id=?',(first.validation_run_id,)).fetchone()
        self.assertEqual(tuple(amount),(98,98))
        report = self.db.execute('select distinct_payment_count,valid_payment_count,validation_pass_rate_pct from validation_daily_reports where validation_run_id=?',(first.validation_run_id,)).fetchone()
        self.assertEqual(tuple(report),(4,1,25))

    def test_broken_pair_stops_one_command_pipeline_before_database_access(self):
        folder = ROOT/'testdaten/batch_scenarios/03_broken_format'
        output = StringIO()
        with redirect_stdout(output),redirect_stderr(output):
            result = run_batch_pipeline.main(['--payment-csv',str(folder/'payments.csv'),'--bank-csv',str(folder/'bank.csv')])
        self.assertEqual(result,1)
        self.assertIn('No database changes.',output.getvalue())

    def test_wrong_fixture_name_stops_pipeline_before_loading(self):
        folder = ROOT/'testdaten/batch_scenarios/01_initial'
        output = StringIO()
        with redirect_stdout(output),redirect_stderr(output):
            result = run_batch_pipeline.main(['--payment-csv',str(folder/'payments.csv'),'--bank-csv',str(folder/'bank.csv'),'--scenario','02_updated'])
        self.assertEqual(result,1)
        self.assertIn('does not match',output.getvalue())


class BuildProofTests(unittest.TestCase):
    def setUp(self):
        folder = TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.file = self.root/'dbt_validation_pipeline/target/run_results.json'
        self.file.parent.mkdir(parents=True)
        self.data = {'metadata':{'invocation_id':'test','dbt_version':'1.12.2'},
            'args':{'vars':{'batch_id':FIRST}},'results':[
                *[{'unique_id':f'model.example.m{i}','status':'success'} for i in range(6)],
                {'unique_id':'test.example.t1','status':'pass'}]}

    def write(self):
        self.file.write_text(json.dumps(self.data))

    def test_successful_proof_is_tied_to_selected_batch(self):
        self.write()
        self.assertEqual(build_proof(self.root,FIRST)['models'],6)
        with self.assertRaisesRegex(ValueError,'another batch'):
            build_proof(self.root,UPDATED)

    def test_fail_warn_or_skip_never_counts_as_success(self):
        for status in ['fail','warn','skipped','error']:
            with self.subTest(status=status):
                self.data['results'][-1]['status']=status
                self.write()
                with self.assertRaises(ValueError):
                    build_proof(self.root,FIRST)

    def test_missing_tests_cannot_prove_success(self):
        self.data['results']=self.data['results'][:-1]
        self.write()
        with self.assertRaises(ValueError):
            build_proof(self.root,FIRST)

    def test_code_hash_changes_with_sql_but_ignores_credentials(self):
        for name in ['dbt_validation_pipeline/dbt_project.yml','validate_batch.py',
            'python_import/result_history.py','sql_views/07_create_validation_history.sql',
            'dbt_validation_pipeline/models/example.sql']:
            file = self.root/name
            file.parent.mkdir(parents=True,exist_ok=True)
            file.write_text('example')
        before = code_fingerprint(self.root)
        (self.root/'.env').write_text('not-a-real-secret')
        self.assertEqual(code_fingerprint(self.root),before)
        (self.root/'dbt_validation_pipeline/models/example.sql').write_text('changed sql')
        self.assertNotEqual(code_fingerprint(self.root),before)


if __name__ == '__main__':
    unittest.main(verbosity=2)
