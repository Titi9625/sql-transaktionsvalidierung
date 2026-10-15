"""Atomic history loads in isolated SQLite; PostgreSQL locking is checked separately."""

from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path
import shutil
import sqlite3
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from python_import.batch_registry import prepare_pair, register_batch
from python_import.csv_contract import CsvFormatError
from python_import.batch_loader import BatchStateError, load_batch
import python_import.batch_loader as loader
import load_input_pair


def translate(query):
    return query.replace('public.','').replace('%s','?').replace('FOR UPDATE','').replace('clock_timestamp()','CURRENT_TIMESTAMP')


def parameters(values):
    return tuple(str(value) if isinstance(value,(date,datetime,Decimal)) else value for value in values)


class CursorAdapter:
    def __init__(self,connection):
        self.cursor = connection.cursor()
    def __enter__(self):
        return self
    def __exit__(self,*args):
        self.cursor.close()
    def execute(self,query,values):
        self.cursor.execute(translate(query),parameters(values))
    def executemany(self,query,rows):
        self.cursor.executemany(translate(query),(parameters(row) for row in rows))
    def fetchone(self):
        return self.cursor.fetchone()


class ConnectionAdapter:
    def __init__(self,connection):
        self.connection = connection
    def cursor(self):
        return CursorAdapter(self.connection)
    def __enter__(self):
        return self
    def __exit__(self,kind,error,traceback):
        self.connection.rollback() if kind else self.connection.commit()


class BatchLoaderTests(unittest.TestCase):
    def setUp(self):
        folder = TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.payment = self.folder/'payments.csv'
        self.bank = self.folder/'bank.csv'
        self.payment.write_text('provider_transaction_id,created,currency,amount,fee,net,status,type,reporting_category,source\ntxn_101,2026-10-01,eur,5000,100,4900,pending,charge,charge,ch_101\ntxn_103,2026-10-01,eur,2000,50,1950,available,charge,charge,ch_103\ntxn_103,2026-10-01,eur,2000,50,1950,available,charge,charge,ch_103\n',encoding='utf-8')
        self.bank.write_text('bank_transaction_id,booking_date,reference_text,amount,currency,counterparty\nbank_103,2026-10-01,STRIPE PAYOUT txn_103,19.50,EUR,Example provider\n',encoding='utf-8')
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.execute('PRAGMA foreign_keys=ON')
        self.db.execute('''CREATE TABLE ingestion_batches (
            batch_id text PRIMARY KEY,contract_version text,pair_fingerprint text UNIQUE,
            payment_fingerprint text,bank_fingerprint text,payment_file_name text,bank_file_name text,
            payment_row_count integer,bank_row_count integer,status text DEFAULT 'registered',
            registered_at text DEFAULT CURRENT_TIMESTAMP,load_started_at text,loaded_at text,last_error_type text)''')
        # Reuse the delivered CREATE TABLE and index statements; SQLite needs
        # the public schema prefix removed and the ALTER migration skipped.
        schema = (ROOT/'sql_views/06_create_batch_history.sql').read_text()
        schema = schema[schema.index('CREATE TABLE'):].replace('public.','')
        self.db.executescript(schema)
        self.adapter = ConnectionAdapter(self.db)

    def counts(self):
        return tuple(self.db.execute(f'select count(*) from {table}').fetchone()[0]
                     for table in ['raw_payment_history','raw_bank_history'])

    def test_registered_pair_loads_both_files_and_becomes_ready(self):
        pair = prepare_pair(self.payment,self.bank)
        with self.adapter:
            registration = register_batch(self.adapter,pair)
        result = load_batch(self.adapter,pair)
        self.assertEqual(result.batch_id,registration.batch_id)
        self.assertEqual((result.payment_rows_inserted,result.bank_rows_inserted),(3,1))
        self.assertEqual(self.counts(),(3,1))
        self.assertEqual(self.db.execute('select status,loaded_at IS NOT NULL from ingestion_batches').fetchone(),('ready',1))

    def test_repeated_pair_skips_and_keeps_every_saved_value(self):
        pair = prepare_pair(self.payment,self.bank)
        first = load_batch(self.adapter,pair)
        before = [self.db.execute(f'select * from {table}').fetchall()
                  for table in ['ingestion_batches','raw_payment_history','raw_bank_history']]
        other = load_batch(self.adapter,pair)
        self.assertTrue(other.skipped)
        self.assertEqual(other.batch_id,first.batch_id)
        self.assertEqual((other.payment_rows_inserted,other.bank_rows_inserted),(0,0))
        after = [self.db.execute(f'select * from {table}').fetchall()
                 for table in ['ingestion_batches','raw_payment_history','raw_bank_history']]
        self.assertEqual(before,after)

    def test_new_snapshot_preserves_old_payment_status(self):
        first = load_batch(self.adapter,prepare_pair(self.payment,self.bank))
        self.payment.write_text(self.payment.read_text().replace('pending','available'),encoding='utf-8')
        other = load_batch(self.adapter,prepare_pair(self.payment,self.bank))
        self.assertNotEqual(first.batch_id,other.batch_id)
        self.assertEqual(self.counts(),(6,2))
        statuses = self.db.execute("select batch_id,status from raw_payment_history where provider_transaction_id='txn_101'").fetchall()
        self.assertEqual(dict(statuses),{first.batch_id:'pending',other.batch_id:'available'})

    def test_duplicate_source_ids_remain_two_distinct_rows(self):
        result = load_batch(self.adapter,prepare_pair(self.payment,self.bank))
        rows = self.db.execute("select source_row_number from raw_payment_history where batch_id=? and provider_transaction_id='txn_103' order by source_row_number",(result.batch_id,)).fetchall()
        self.assertEqual(rows,[(2,),(3,)])

    def test_bank_failure_rolls_back_both_sources_and_allows_retry(self):
        pair = prepare_pair(self.payment,self.bank)
        original = loader._insert_bank_history
        def insert_then_fail(cursor,batch_id,dataset):
            original(cursor,batch_id,dataset)
            raise ValueError('Deliberate failure after the bank insert')
        with patch.object(loader,'_insert_bank_history',side_effect=insert_then_fail):
            with self.assertRaises(ValueError):
                load_batch(self.adapter,pair)
        self.assertEqual(self.counts(),(0,0))
        batch_id,status,error = self.db.execute('select batch_id,status,last_error_type from ingestion_batches').fetchone()
        self.assertEqual((status,error),('failed','ValueError'))
        retried = load_batch(self.adapter,pair)
        self.assertEqual(retried.batch_id,batch_id)
        self.assertEqual(self.counts(),(3,1))
        self.assertEqual(self.db.execute('select status,last_error_type from ingestion_batches').fetchone(),('ready',None))

    def test_failed_new_snapshot_does_not_change_older_history(self):
        first = load_batch(self.adapter,prepare_pair(self.payment,self.bank))
        before = self.db.execute('select * from raw_payment_history where batch_id=?',(first.batch_id,)).fetchall()
        self.payment.write_text(self.payment.read_text().replace('pending','available'),encoding='utf-8')
        with patch.object(loader,'_insert_bank_history',side_effect=ValueError('Deliberate bank failure')):
            with self.assertRaises(ValueError):
                load_batch(self.adapter,prepare_pair(self.payment,self.bank))
        self.assertEqual(self.counts(),(3,1))
        self.assertEqual(self.db.execute('select * from raw_payment_history where batch_id=?',(first.batch_id,)).fetchall(),before)
        self.assertEqual(self.db.execute('select status from ingestion_batches where batch_id=?',(first.batch_id,)).fetchone()[0],'ready')

    def test_renamed_identical_files_skip_loading(self):
        first = load_batch(self.adapter,prepare_pair(self.payment,self.bank))
        payment,bank = self.folder/'renamed_pay.csv',self.folder/'renamed_bank.csv'
        shutil.copyfile(self.payment,payment)
        shutil.copyfile(self.bank,bank)
        other = load_batch(self.adapter,prepare_pair(payment,bank))
        self.assertTrue(other.skipped)
        self.assertEqual(other.batch_id,first.batch_id)

    def test_inconsistent_ready_batch_is_reported_without_reloading(self):
        pair = prepare_pair(self.payment,self.bank)
        load_batch(self.adapter,pair)
        self.db.execute('delete from raw_bank_history')
        self.db.commit()
        with self.assertRaises(BatchStateError):
            load_batch(self.adapter,pair)
        self.assertEqual(self.counts(),(3,0))
        self.assertEqual(self.db.execute('select status from ingestion_batches').fetchone()[0],'ready')

    def test_missing_amount_and_zero_are_preserved_in_history(self):
        self.payment.write_text(self.payment.read_text().replace('5000,100,4900',',0,0'),encoding='utf-8')
        load_batch(self.adapter,prepare_pair(self.payment,self.bank))
        self.assertEqual(self.db.execute("select amount,fee,net from raw_payment_history where provider_transaction_id='txn_101'").fetchone(),(None,0,0))

    def test_bad_format_exits_before_schema_or_load(self):
        self.bank.write_text(self.bank.read_text().replace('19.50','NOT_A_NUMBER'),encoding='utf-8')
        output = StringIO()
        with redirect_stdout(output),redirect_stderr(output):
            result = load_input_pair.main(['--payment-csv',str(self.payment),'--bank-csv',str(self.bank),'--init-schema'])
        self.assertEqual(result,1)
        self.assertIn('No database changes.',output.getvalue())
        self.assertEqual(self.counts(),(0,0))

    def test_sample_initial_repeat_updated_and_broken_pair(self):
        scenarios = ROOT/'testdaten/batch_scenarios'
        def pair(name):
            return prepare_pair(scenarios/name/'payments.csv',scenarios/name/'bank.csv')
        first = load_batch(self.adapter,pair('01_initial'))
        self.assertEqual(self.counts(),(5,3))
        repeat = load_batch(self.adapter,pair('01_initial'))
        self.assertTrue(repeat.skipped)
        self.assertEqual(repeat.batch_id,first.batch_id)
        self.assertEqual(self.counts(),(5,3))
        updated = load_batch(self.adapter,pair('02_updated'))
        self.assertNotEqual(updated.batch_id,first.batch_id)
        self.assertEqual(self.counts(),(10,7))
        with self.assertRaises(CsvFormatError):
            pair('03_broken_format')
        self.assertEqual(self.counts(),(10,7))
        original_status = self.db.execute("select status from raw_payment_history where batch_id=? and provider_transaction_id='txn_102'",(first.batch_id,)).fetchone()[0]
        self.assertEqual(original_status,'pending')


if __name__ == '__main__':
    unittest.main(verbosity=2)
