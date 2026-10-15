"""CSV boundary checks. Uses temporary files and no database connection."""

from contextlib import redirect_stderr, redirect_stdout
import csv
from decimal import Decimal
from io import StringIO
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from python_import.csv_contract import BANK_COLUMNS, PAYMENT_COLUMNS, CsvFormatError, read_csv_dataset, read_input_pair
import check_input_pair


class CsvContractTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def payment(self, **changes):
        row = dict(zip(PAYMENT_COLUMNS, ['txn_101','2026-10-01','eur','10000','200','9800','available','charge','charge','ch_101']))
        row.update(changes)
        return row

    def bank(self, **changes):
        row = dict(zip(BANK_COLUMNS, ['bank_101','2026-10-02','STRIPE PAYOUT txn_101','98.00','EUR','Example, provider']))
        row.update(changes)
        return row

    def write(self, name, columns, rows):
        path = self.root / name
        with path.open('w', encoding='utf-8', newline='') as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        return path

    def test_keeps_exact_amounts_dates_and_quoted_fields(self):
        pay = self.write('payment.csv', PAYMENT_COLUMNS, [self.payment()])
        bank = self.write('bank.csv', BANK_COLUMNS, [self.bank()])
        payment, banking = read_input_pair(pay, bank)
        self.assertEqual(payment.rows[0]['net'], 9800)
        self.assertEqual(str(payment.rows[0]['created']), '2026-10-01')
        self.assertEqual(banking.rows[0]['amount'], Decimal('98.00'))
        self.assertEqual(banking.rows[0]['counterparty'], 'Example, provider')

    def test_missing_amount_is_none_but_zero_is_zero(self):
        pay = self.write('payment.csv', PAYMENT_COLUMNS, [self.payment(amount='',fee='0',net='0')])
        row = read_csv_dataset(pay, 'payment').rows[0]
        self.assertIsNone(row['amount'])
        self.assertEqual(row['fee'], 0)
        self.assertEqual(row['net'], 0)

    def test_preserves_duplicates_pending_refunds_and_non_eur(self):
        original = self.payment()
        pay = self.write('payment.csv', PAYMENT_COLUMNS, [original, original, self.payment(provider_transaction_id='txn_102',status='pending',type='refund',currency='usd',amount='')])
        rows = read_csv_dataset(pay, 'payment').rows
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], rows[1])
        self.assertEqual((rows[2]['status'], rows[2]['type'], rows[2]['currency']), ('pending','refund','usd'))
        self.assertIsNone(rows[2]['amount'])

    def test_payment_amounts_never_truncate_or_overflow(self):
        for amount in ['4900.9','4900.0','1e3','NaN','Infinity','2147483648','-2147483649']:
            with self.subTest(amount=amount):
                pay = self.write('payment.csv', PAYMENT_COLUMNS, [self.payment(net=amount)])
                with self.assertRaises(CsvFormatError):
                    read_csv_dataset(pay, 'payment')

    def test_integer_boundaries_are_preserved(self):
        pay = self.write('payment.csv', PAYMENT_COLUMNS, [self.payment(amount='2147483647',net='-2147483648')])
        row = read_csv_dataset(pay, 'payment').rows[0]
        self.assertEqual(row['amount'],2147483647)
        self.assertEqual(row['net'],-2147483648)

    def test_bank_amounts_reject_text_excess_decimals_and_overflow(self):
        for amount in ['NOT_A_NUMBER','NaN','Infinity','49.001','49,00','10000000000.00']:
            with self.subTest(amount=amount):
                bank = self.write('bank.csv', BANK_COLUMNS, [self.bank(amount=amount)])
                with self.assertRaises(CsvFormatError):
                    read_csv_dataset(bank,'bank')

    def test_bank_missing_zero_and_negative_amounts_stay_distinct(self):
        bank = self.write('bank.csv', BANK_COLUMNS, [self.bank(amount=''),self.bank(amount='0.00'),self.bank(amount='-49.00')])
        amounts = [row['amount'] for row in read_csv_dataset(bank,'bank').rows]
        self.assertEqual(amounts,[None,Decimal('0.00'),Decimal('-49.00')])

    def test_requires_supported_headers_but_allows_reordering(self):
        pay = self.write('payment.csv', tuple(reversed(PAYMENT_COLUMNS)), [self.payment()])
        self.assertEqual(read_csv_dataset(pay,'payment').rows[0]['net'],9800)
        pay.write_text('provider_transaction_id,net\ntxn_101,9800\n',encoding='utf-8')
        with self.assertRaisesRegex(CsvFormatError,'missing columns'):
            read_csv_dataset(pay,'payment')
        pay.write_text(','.join(PAYMENT_COLUMNS)+',net\n',encoding='utf-8')
        with self.assertRaisesRegex(CsvFormatError,'duplicate column'):
            read_csv_dataset(pay,'payment')

    def test_date_must_be_iso_and_exist(self):
        for value in ['20261001','01/10/2026','2026-02-30','']:
            with self.subTest(value=value):
                pay = self.write('payment.csv',PAYMENT_COLUMNS,[self.payment(created=value)])
                with self.assertRaises(CsvFormatError):
                    read_csv_dataset(pay,'payment')

    def test_required_identifiers_and_status_are_present(self):
        for column in ['provider_transaction_id','currency','status','type']:
            with self.subTest(column=column):
                pay = self.write('payment.csv',PAYMENT_COLUMNS,[self.payment(**{column:''})])
                with self.assertRaisesRegex(CsvFormatError,'required value is empty'):
                    read_csv_dataset(pay,'payment')

    def test_bad_field_count_and_unterminated_quotes_fail(self):
        pay = self.root / 'payment.csv'
        for data in ['txn_101,2026-10-01\n','"unterminated\n']:
            with self.subTest(data=data):
                pay.write_text(','.join(PAYMENT_COLUMNS)+'\n'+data,encoding='utf-8')
                with self.assertRaises(CsvFormatError):
                    read_csv_dataset(pay,'payment')

    def test_empty_file_or_snapshot_and_missing_file_fail(self):
        pay = self.root / 'payment.csv'
        with self.assertRaises(CsvFormatError):
            read_csv_dataset(pay,'payment')
        for content in ['',','.join(PAYMENT_COLUMNS)+'\n']:
            with self.subTest(content=content):
                pay.write_text(content,encoding='utf-8')
                with self.assertRaises(CsvFormatError):
                    read_csv_dataset(pay,'payment')

    def test_utf8_bom_supported_invalid_encoding_rejected(self):
        pay = self.write('payment.csv',PAYMENT_COLUMNS,[self.payment()])
        pay.write_bytes(b'\xef\xbb\xbf'+pay.read_bytes())
        self.assertEqual(len(read_csv_dataset(pay,'payment').rows),1)
        pay.write_bytes(b'\xff\xfeinvalid')
        with self.assertRaises(CsvFormatError):
            read_csv_dataset(pay,'payment')

    def test_cli_checks_both_files_and_reports_bank_failure(self):
        pay = self.write('payments.csv',PAYMENT_COLUMNS,[self.payment()])
        bank = self.write('bank.csv',BANK_COLUMNS,[self.bank(amount='NOT_A_NUMBER')])
        output = StringIO()
        with redirect_stderr(output), redirect_stdout(output):
            result = check_input_pair.main(['--payment-csv',str(pay),'--bank-csv',str(bank)])
        self.assertEqual(result,1)
        self.assertIn('bank.csv: data row 1, column amount',output.getvalue())
        self.assertIn('No database changes.',output.getvalue())

    def test_cli_success_and_incomplete_pair(self):
        pay = self.write('payments.csv',PAYMENT_COLUMNS,[self.payment()])
        bank = self.write('bank.csv',BANK_COLUMNS,[self.bank()])
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(check_input_pair.main(['--payment-csv',str(pay),'--bank-csv',str(bank)]),0)
        self.assertIn('Input format check passed.',output.getvalue())
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit) as error:
            check_input_pair.main(['--payment-csv',str(pay)])
        self.assertEqual(error.exception.code,2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
