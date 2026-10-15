"""Check a selected CSV pair without importing it or changing the database."""

import argparse
from pathlib import Path
import sys

from python_import.csv_contract import CsvFormatError, read_input_pair


ROOT = Path(__file__).resolve().parent


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--payment-csv', type=Path, help='Payment file; supply together with --bank-csv')
    parser.add_argument('--bank-csv', type=Path, help='Bank file; supply together with --payment-csv')
    args = parser.parse_args(argv)
    if (args.payment_csv is None) != (args.bank_csv is None):
        parser.error('supply both --payment-csv and --bank-csv, or neither for the original fixtures')
    payment_path = args.payment_csv if args.payment_csv is not None else ROOT / 'testdaten/sample_stripe_transactions_large.csv'
    bank_path = args.bank_csv if args.bank_csv is not None else ROOT / 'testdaten/sample_bank_transactions_large.csv'
    try:
        payment, bank = read_input_pair(payment_path, bank_path)
    except CsvFormatError as error:
        print(f'Input format check failed: {error}', file=sys.stderr)
        print('No database changes.', file=sys.stderr)
        return 1
    print(f'Payment file: {payment.path.name} ({len(payment.rows)} rows)')
    print(f'Bank file: {bank.path.name} ({len(bank.rows)} rows)')
    print('Input format check passed. No database changes.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
