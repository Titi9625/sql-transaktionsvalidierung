"""One command: check a file pair, load history, validate and save results."""

import argparse
from pathlib import Path
import sys

from python_import.batch_registry import prepare_pair
from python_import.batch_loader import load_batch
from python_import.csv_contract import CsvFormatError
import validate_batch


ROOT = Path(__file__).resolve().parent


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--payment-csv',required=True,type=Path)
    parser.add_argument('--bank-csv',required=True,type=Path)
    parser.add_argument('--scenario',choices=['01_initial','02_updated'])
    args = parser.parse_args(argv)
    try:
        pair = prepare_pair(args.payment_csv,args.bank_csv)
        # Reject a mismatched scenario before loading any raw rows.
        expected,_ = validate_batch.scenario_expectations(args.scenario)
        if expected is not None and expected != pair.pair_fingerprint:
            raise ValueError('The named scenario does not match the supplied file pair.')
    except (CsvFormatError,OSError,ValueError,KeyError,TypeError) as error:
        print(f'Input pair rejected: {error}',file=sys.stderr)
        print('No database changes.',file=sys.stderr)
        return 1
    print('[1/3] Both input files are readable.',flush=True)
    try:
        import psycopg2
        from dotenv import dotenv_values
    except ImportError:
        print('Use the repository .venv Python.',file=sys.stderr)
        return 1
    settings = dotenv_values(ROOT/'.env')
    if any(not settings.get(key) for key in ['DB_HOST','DB_PORT','DB_NAME','DB_USER','DB_PASSWORD']):
        print('Required database settings are missing from .env.',file=sys.stderr)
        return 1
    if settings['DB_NAME']!='portfolio_validation':
        print('This pipeline requires DB_NAME=portfolio_validation.',file=sys.stderr)
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
            with connection.cursor() as cursor:
                for file in ['05_create_ingestion_batches.sql','06_create_batch_history.sql']:
                    cursor.execute((ROOT/'sql_views'/file).read_text(encoding='utf-8'))
        loaded = load_batch(connection,pair)
    except Exception as error:
        print(f'Pipeline stopped during raw loading ({type(error).__name__}).',file=sys.stderr)
        return 1
    finally:
        if connection is not None:
            connection.close()
    print(f'[2/3] Batch {loaded.batch_id}: {loaded.payment_rows_inserted} payment rows and {loaded.bank_rows_inserted} bank rows added.',flush=True)
    print('[3/3] Validate this batch and save the successful results.',flush=True)
    options = ['--batch-id',loaded.batch_id,'--save-results','--init-history']
    if args.scenario:
        options.extend(['--scenario',args.scenario])
    result = validate_batch.main(options)
    if result == 0:
        print('Batch pipeline completed successfully.')
    return result


if __name__ == '__main__':
    raise SystemExit(main())
