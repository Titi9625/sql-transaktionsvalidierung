"""File identity and registry behavior in an isolated SQLite database.

SQLite adapts only public schema naming and placeholders. Live PostgreSQL
schema/types/concurrency are verified separately by the registration commands.
No database credentials are used here.
"""

from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
import shutil
import sqlite3
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from python_import.batch_registry import CONTRACT_VERSION, pair_fingerprint, prepare_pair, register_batch
from python_import.csv_contract import CsvFormatError
import register_input_pair


class CursorAdapter:
    def __init__(self, connection):
        self.cursor = connection.cursor()
    def __enter__(self):
        return self
    def __exit__(self,*args):
        self.cursor.close()
    def execute(self,query,parameters):
        self.cursor.execute(query.replace('public.','').replace('%s','?'),parameters)
    def fetchone(self):
        return self.cursor.fetchone()


class ConnectionAdapter:
    def __init__(self, connection):
        self.connection = connection
    def cursor(self):
        return CursorAdapter(self.connection)


class BatchRegistryTests(unittest.TestCase):
    def setUp(self):
        folder = TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.folder = Path(folder.name)
        self.payment = self.folder / 'payment.csv'
        self.bank = self.folder / 'bank.csv'
        self.payment.write_text('provider_transaction_id,created,currency,amount,fee,net,status,type,reporting_category,source\ntxn_101,2026-10-01,eur,5000,100,4900,available,charge,charge,ch_101\n',encoding='utf-8')
        self.bank.write_text('bank_transaction_id,booking_date,reference_text,amount,currency,counterparty\nbank_101,2026-10-02,STRIPE PAYOUT txn_101,49.00,EUR,Example provider\n',encoding='utf-8')

    def registry(self):
        database = sqlite3.connect(':memory:')
        self.addCleanup(database.close)
        database.execute('''CREATE TABLE ingestion_batches (
            batch_id text PRIMARY KEY, contract_version text,
            pair_fingerprint text UNIQUE, payment_fingerprint text,
            bank_fingerprint text, payment_file_name text, bank_file_name text,
            payment_row_count integer, bank_row_count integer,
            status text DEFAULT 'registered',
            registered_at text DEFAULT CURRENT_TIMESTAMP)''')
        return database, ConnectionAdapter(database)

    def test_renaming_identical_files_preserves_pair_identity(self):
        first = prepare_pair(self.payment,self.bank)
        other_payment, other_bank = self.folder/'renamed_payment.csv', self.folder/'renamed_bank.csv'
        shutil.copyfile(self.payment,other_payment)
        shutil.copyfile(self.bank,other_bank)
        other = prepare_pair(other_payment,other_bank)
        self.assertEqual(first.pair_fingerprint,other.pair_fingerprint)

    def test_changing_only_partner_file_creates_new_pair(self):
        first = prepare_pair(self.payment,self.bank)
        self.bank.write_text(self.bank.read_text().replace('49.00','48.99'),encoding='utf-8')
        other = prepare_pair(self.payment,self.bank)
        self.assertEqual(first.payment_fingerprint,other.payment_fingerprint)
        self.assertNotEqual(first.bank_fingerprint,other.bank_fingerprint)
        self.assertNotEqual(first.pair_fingerprint,other.pair_fingerprint)

    def test_source_roles_and_contract_version_affect_identity(self):
        self.assertNotEqual(pair_fingerprint('a','b'),pair_fingerprint('b','a'))
        self.assertNotEqual(pair_fingerprint('a','b'),pair_fingerprint('a','b',CONTRACT_VERSION+'_new'))

    def test_byte_changes_are_new_identity_even_with_same_parsed_values(self):
        first = prepare_pair(self.payment,self.bank)
        self.payment.write_bytes(self.payment.read_bytes().replace(b'\n',b'\r\n'))
        other = prepare_pair(self.payment,self.bank)
        self.assertEqual(first.payment.rows,other.payment.rows)
        self.assertNotEqual(first.pair_fingerprint,other.pair_fingerprint)

    def test_validated_rows_and_fingerprint_use_captured_bytes(self):
        first = prepare_pair(self.payment,self.bank)
        self.payment.write_text(self.payment.read_text().replace('available','pending'),encoding='utf-8')
        other = prepare_pair(self.payment,self.bank)
        self.assertEqual(first.payment.rows[0]['status'],'available')
        self.assertEqual(other.payment.rows[0]['status'],'pending')
        self.assertNotEqual(first.pair_fingerprint,other.pair_fingerprint)

    def test_repeated_registration_reuses_id_and_does_not_change_row(self):
        database, adapter = self.registry()
        pair = prepare_pair(self.payment,self.bank)
        first = register_batch(adapter,pair)
        before = database.execute('select * from ingestion_batches').fetchall()
        other = register_batch(adapter,pair)
        self.assertTrue(first.created)
        self.assertFalse(other.created)
        self.assertEqual(first.batch_id,other.batch_id)
        self.assertEqual(database.execute('select * from ingestion_batches').fetchall(),before)
        self.assertEqual(other.status,'registered')

    def test_updated_pair_gets_new_id_and_preserves_earlier_metadata(self):
        database, adapter = self.registry()
        first = register_batch(adapter,prepare_pair(self.payment,self.bank))
        self.payment.write_text(self.payment.read_text().replace('available','pending'),encoding='utf-8')
        other = register_batch(adapter,prepare_pair(self.payment,self.bank))
        self.assertTrue(other.created)
        self.assertNotEqual(first.batch_id,other.batch_id)
        self.assertEqual(database.execute('select count(*) from ingestion_batches').fetchone()[0],2)

    def test_known_failed_batch_is_not_treated_as_completed(self):
        database, adapter = self.registry()
        pair = prepare_pair(self.payment,self.bank)
        first = register_batch(adapter,pair)
        database.execute("update ingestion_batches set status='failed'")
        other = register_batch(adapter,pair)
        self.assertEqual(other.batch_id,first.batch_id)
        self.assertEqual(other.status,'failed')
        self.assertFalse(other.created)

    def test_broken_bank_is_rejected_before_registration(self):
        self.bank.write_text(self.bank.read_text().replace('49.00','NOT_A_NUMBER'),encoding='utf-8')
        output = StringIO()
        with redirect_stdout(output),redirect_stderr(output):
            result = register_input_pair.main(['--payment-csv',str(self.payment),'--bank-csv',str(self.bank),'--init-schema'])
        self.assertEqual(result,1)
        self.assertIn('No database changes.',output.getvalue())
        self.assertNotIn('Batch ID:',output.getvalue())

    def test_preview_does_not_need_database_dependencies(self):
        output = StringIO()
        with redirect_stdout(output):
            result = register_input_pair.main(['--payment-csv',str(self.payment),'--bank-csv',str(self.bank),'--preview'])
        self.assertEqual(result,0)
        self.assertIn('Preview only. No database changes.',output.getvalue())


if __name__ == '__main__':
    unittest.main(verbosity=2)
