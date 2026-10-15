"""Register a readable file pair, without importing transaction rows."""

import argparse
from pathlib import Path
import sys

from python_import.batch_registry import prepare_pair, register_batch
from python_import.csv_contract import CsvFormatError


ROOT = Path(__file__).resolve().parent


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--payment-csv', type=Path)
    parser.add_argument('--bank-csv', type=Path)
    parser.add_argument('--init-schema', action='store_true', help='Create the metadata table if missing')
    parser.add_argument('--preview', action='store_true', help='Show fingerprints without connecting to PostgreSQL')
    args = parser.parse_args(argv)
    if (args.payment_csv is None) != (args.bank_csv is None):
        parser.error('supply both --payment-csv and --bank-csv, or neither for the original fixtures')
    if args.preview and args.init_schema:
        parser.error('--preview does not create tables; omit --init-schema')
    payment_path = args.payment_csv if args.payment_csv is not None else ROOT / 'testdaten/sample_stripe_transactions_large.csv'
    bank_path = args.bank_csv if args.bank_csv is not None else ROOT / 'testdaten/sample_bank_transactions_large.csv'
    try:
        pair = prepare_pair(payment_path, bank_path)
    except CsvFormatError as error:
        print(f'Input format check failed: {error}',file=sys.stderr)
        print('No database changes.',file=sys.stderr)
        return 1
    print(f'Payment file: {pair.payment.path.name} ({len(pair.payment.rows)} rows)',flush=True)
    print(f'Bank file: {pair.bank.path.name} ({len(pair.bank.rows)} rows)',flush=True)
    if args.preview:
        print(f'Pair fingerprint: {pair.pair_fingerprint}')
        print('Preview only. No database changes.')
        return 0
    try:
        import psycopg2
        from dotenv import dotenv_values
    except ImportError:
        print('Run this command with the repository .venv Python; psycopg2 and python-dotenv are required.',file=sys.stderr)
        return 1
    settings = dotenv_values(ROOT / '.env')
    for key in ('DB_HOST','DB_PORT','DB_NAME','DB_USER','DB_PASSWORD'):
        if not settings.get(key):
            print(f'Missing .env setting: {key}',file=sys.stderr)
            return 1
    if settings['DB_NAME'] != 'portfolio_validation':
        print('This portfolio registration step requires DB_NAME=portfolio_validation.',file=sys.stderr)
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
        with connection:
            if args.init_schema:
                schema = (ROOT / 'sql_views/05_create_ingestion_batches.sql').read_text(encoding='utf-8')
                with connection.cursor() as cursor:
                    cursor.execute(schema)
            result = register_batch(connection,pair)
    except Exception as error:
        print(f'Batch registration failed ({type(error).__name__}).',file=sys.stderr)
        if getattr(error,'pgcode',None) == '42P01':
            print('The batch table is missing. Run again with --init-schema.',file=sys.stderr)
        print('No transaction rows were imported.',file=sys.stderr)
        return 1
    finally:
        if connection is not None:
            connection.close()
    print('New pair registered.' if result.created else 'Existing pair recognised.')
    print(f'Batch ID: {result.batch_id}')
    print(f'Status: {result.status}')
    print('Metadata only. No transaction rows were imported.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
