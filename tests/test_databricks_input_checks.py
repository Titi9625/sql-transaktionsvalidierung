"""Check the actual notebook's portable input logic, without a Spark runtime.

These tests do not exercise Delta MERGE, concurrency, or cloud execution.
The live Databricks load and repeat checks must be verified separately.
"""
import ast
import os
from pathlib import Path
import unittest


ROOT = Path(os.environ.get('PORTFOLIO_TEST_ROOT', Path(__file__).resolve().parents[1]))
NOTEBOOK = Path(os.environ.get('DATABRICKS_NOTEBOOK_UNDER_TEST', ROOT / 'databricks/notebooks/01_ingest_bronze.py'))


def portable_functions():
    tree = ast.parse(NOTEBOOK.read_text(encoding='utf-8'))
    definitions = [node for node in tree.body if isinstance(node, (ast.Import, ast.ImportFrom, ast.FunctionDef))]
    namespace = {}
    exec(compile(ast.Module(body=definitions, type_ignores=[]), str(NOTEBOOK), 'exec'), namespace)
    return namespace


class BronzeInputChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logic = portable_functions()

    def pair(self, scenario='01_initial'):
        folder = ROOT / 'testdaten/batch_scenarios' / scenario
        capture = self.logic['capture_source']
        payment = capture(folder/'payments.csv', ('provider_transaction_id','created','currency',
            'amount','fee','net','status','type','reporting_category','source'))
        bank = capture(folder/'bank.csv', ('bank_transaction_id','booking_date','reference_text',
            'amount','currency','counterparty'))
        return payment, bank

    def test_initial_identity_and_duplicate_positions(self):
        payment, bank = self.pair()
        result = self.logic['describe_bronze_pair'](payment, bank, 'csv_pair_v1')
        self.assertEqual((result['payment_row_count'], result['bank_row_count']), (5, 3))
        # Byte-based identities may differ after a checkout that changes line endings.
        self.assertRegex(result['batch_id'], r'^[0-9a-f]{64}$')
        self.assertEqual(result['batch_id'], result['pair_fingerprint'])
        self.assertEqual([row['source_row_number'] for row in payment['rows']
                          if row['provider_transaction_id']=='txn_103'], [3, 4])

    def test_updated_snapshot_has_new_identity(self):
        first = self.logic['describe_bronze_pair'](*self.pair(), 'csv_pair_v1')
        second = self.logic['describe_bronze_pair'](*self.pair('02_updated'), 'csv_pair_v1')
        self.assertNotEqual(first['batch_id'], second['batch_id'])
        self.assertEqual(second['bank_row_count'], 4)

    def test_renaming_does_not_change_identity(self):
        payment, bank = self.pair()
        first = self.logic['describe_bronze_pair'](payment, bank, 'csv_pair_v1')
        payment['file_name'], bank['file_name'] = 'renamed_payment.csv', 'renamed_bank.csv'
        self.assertEqual(self.logic['describe_bronze_pair'](payment, bank, 'csv_pair_v1')['batch_id'], first['batch_id'])

    def test_broken_bank_amount_is_rejected_before_spark_access(self):
        payment, bank = self.pair('03_broken_format')
        with self.assertRaisesRegex(ValueError, 'numeric'):
            self.logic['load_bronze_pair'](None, payment, bank, 'csv_pair_v1', 'unused')

    def test_missing_zero_pending_refund_and_currency_are_preserved(self):
        payment, bank = self.pair()
        payment['rows'][0].update(amount=None, net='0', fee='0', currency='USD', type='refund')
        self.logic['describe_bronze_pair'](payment, bank, 'csv_pair_v1')
        self.assertIsNone(payment['rows'][0]['amount'])
        self.assertEqual(payment['rows'][0]['net'], '0')
        self.assertEqual(payment['rows'][1]['status'], 'pending')

    def test_invalid_dates_ranges_and_formats_are_rejected(self):
        for role, field, value in [('payment','created','2026-02-30'),
            ('payment','amount','49.5'), ('payment','amount','2147483648'),
            ('payment','provider_transaction_id',None), ('bank','amount','49.001'),
            ('bank','amount','10000000000.00')]:
            with self.subTest(role=role, field=field, value=value):
                pair = list(self.pair())
                index = 0 if role == 'payment' else 1
                pair[index]['rows'][0][field] = value
                with self.assertRaises(ValueError):
                    self.logic['describe_bronze_pair'](*pair, 'csv_pair_v1')

    def test_source_positions_and_batch_identity_cannot_be_silently_changed(self):
        payment, bank = self.pair()
        with self.assertRaisesRegex(ValueError, 'rerun Cell 3'):
            self.logic['load_bronze_pair'](None, payment, bank, 'csv_pair_v1', 'wrong')
        payment['rows'][1]['source_row_number'] = 1
        with self.assertRaisesRegex(ValueError, 'position'):
            self.logic['describe_bronze_pair'](payment, bank, 'csv_pair_v1')


if __name__ == '__main__':
    unittest.main(verbosity=2)
