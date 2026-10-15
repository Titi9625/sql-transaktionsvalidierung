"""Load a selected CSV pair into history while preserving earlier batches."""

import argparse
from pathlib import Path
import sys

from python_import.batch_registry import prepare_pair
from python_import.batch_loader import load_batch
from python_import.csv_contract import CsvFormatError


ROOT = Path(__file__).resolve().parent


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--payment-csv',type=Path)
    parser.add_argument('--bank-csv',type=Path)
    parser.add_argument('--init-schema',action='store_true',help='Create batch/history tables if missing')
    args = parser.parse_args(argv)
    if (args.payment_csv is None) != (args.bank_csv is None):
        parser.error('supply both --payment-csv and --bank-csv, or neither for the original fixtures')
    payment_path = args.payment_csv if args.payment_csv is not None else ROOT / 'testdaten/sample_stripe_transactions_large.csv'
    bank_path = args.bank_csv if args.bank_csv is not None else ROOT / 'testdaten/sample_bank_transactions_large.csv'
    try:
        pair = prepare_pair(payment_path,bank_path)
    except CsvFormatError as error:
        print(f'Input format check failed: {error}',file=sys.stderr)
        print('No database changes.',file=sys.stderr)
        return 1
    try:
        import psycopg2
        from dotenv import dotenv_values
    except ImportError:
        print('Use the repository .venv Python; psycopg2 and python-dotenv are required.',file=sys.stderr)
        return 1
    settings = dotenv_values(ROOT / '.env')
    for key in ('DB_HOST','DB_PORT','DB_NAME','DB_USER','DB_PASSWORD'):
        if not settings.get(key):
            print(f'Missing .env setting: {key}',file=sys.stderr)
            return 1
    if settings['DB_NAME'] != 'portfolio_validation':
        print('This batch loading step requires DB_NAME=portfolio_validation.',file=sys.stderr)
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
        if args.init_schema:
            with connection:
                with connection.cursor() as cursor:
                    for file in ['05_create_ingestion_batches.sql','06_create_batch_history.sql']:
                        cursor.execute((ROOT / 'sql_views' / file).read_text(encoding='utf-8'))
        result = load_batch(connection,pair)
    except Exception as error:
        print(f'Batch loading failed ({type(error).__name__}).',file=sys.stderr)
        if getattr(error,'pgcode',None) in {'42P01','42703'}:
            print('Batch history schema is missing or incomplete. Run again with --init-schema.',file=sys.stderr)
        print('This attempt did not publish a new ready batch.',file=sys.stderr)
        return 1
    finally:
        if connection is not None:
            connection.close()
    print('Already loaded. No raw rows added.' if result.skipped else 'Batch loaded successfully.')
    print(f'Batch ID: {result.batch_id}')
    print(f'Status: {result.status} (both source files loaded; ready for validation)')
    print(f'Payment history rows inserted: {result.payment_rows_inserted}')
    print(f'Bank history rows inserted: {result.bank_rows_inserted}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
