"""Acceptance check for the disposable GitHub Actions database only.

Run after the initial pair twice and the updated pair once. This script is
separate from unittest discovery because its exact counts require a fresh DB.
"""

import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from python_import.batch_registry import prepare_pair
import validate_batch


TABLES = (
    'ingestion_batches', 'raw_payment_history', 'raw_bank_history',
    'validation_runs', 'validation_summaries', 'validation_valid_payments',
    'validation_findings', 'validation_daily_reports',
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def snapshot(connection):
    """Compare every saved field, not just counts, around malformed input."""
    with connection.cursor() as cursor:
        result = {}
        for table in TABLES:
            cursor.execute(f'SELECT row_to_json(t)::text FROM public.{table} t')
            result[table] = sorted(row[0] for row in cursor.fetchall())
        return result


def verify(connection):
    before = snapshot(connection)
    require([len(before[name]) for name in TABLES[:4]] == [2, 10, 7, 3],
            'Expected two batches, 10/7 raw rows and three successful validation runs.')
    fixtures = ROOT / 'testdaten/batch_scenarios'
    expected = json.loads((fixtures / 'expected_results.json').read_text(encoding='utf-8'))
    with connection.cursor() as cursor:
        for scenario, run_count in [('01_initial', 2), ('02_updated', 1)]:
            pair = prepare_pair(fixtures/scenario/'payments.csv', fixtures/scenario/'bank.csv')
            cursor.execute('SELECT batch_id,status FROM public.ingestion_batches WHERE pair_fingerprint=%s',
                           (pair.pair_fingerprint,))
            batch = cursor.fetchone()
            require(batch is not None and batch[1] == 'ready', f'{scenario}: batch is not ready.')
            cursor.execute('''SELECT validation_run_id,dbt_models_built,dbt_tests_passed
                FROM public.validation_runs WHERE batch_id=%s''', (batch[0],))
            runs = cursor.fetchall()
            require(len(runs) == run_count, f'{scenario}: unexpected saved run count.')
            values = expected['scenarios'][scenario]['validation']
            for run_id, models, tests in runs:
                require((models, tests) == (6, 30), 'Saved dbt build evidence is incomplete.')
                fields = ','.join(validate_batch.SUMMARY_FIELDS)
                cursor.execute(f'SELECT {fields} FROM public.validation_summaries WHERE validation_run_id=%s', (run_id,))
                require(cursor.fetchall() == [tuple(values[key] for key in validate_batch.SUMMARY_FIELDS)],
                        f'{scenario}: saved summary is incorrect.')
                cursor.execute('''SELECT transaction_id FROM public.validation_valid_payments
                    WHERE validation_run_id=%s ORDER BY transaction_id''', (run_id,))
                require([row[0] for row in cursor.fetchall()] == values['valid_transaction_ids'],
                        f'{scenario}: saved valid IDs are incorrect.')
                cursor.execute('''SELECT error_code,count(*) FROM public.validation_findings
                    WHERE validation_run_id=%s GROUP BY error_code''', (run_id,))
                require(dict(cursor.fetchall()) == values['error_counts'],
                        f'{scenario}: findings were lost, duplicated or misclassified.')
                cursor.execute('''SELECT sum(payment_row_count),sum(distinct_payment_count),
                    sum(valid_payment_count),sum(flagged_payment_count),sum(payment_error_row_count)
                    FROM public.validation_daily_reports WHERE validation_run_id=%s''', (run_id,))
                require(tuple(cursor.fetchone()) == (5, 4, values['valid_records'],
                        values['invalid_payment_transactions'], values['total_validation_errors']),
                        f'{scenario}: saved report metrics are incorrect.')
        cursor.execute('''SELECT status FROM public.raw_payment_history
            WHERE batch_id=%s AND provider_transaction_id='txn_102' ''', (batch[0],))
        require(cursor.fetchall() == [('available',)], 'Updated raw status was not preserved.')
        cursor.execute('SELECT total_payment_records,total_bank_records,valid_records,total_validation_errors FROM public.dbt_validation_summary')
        require(cursor.fetchall() == [(50, 40, 20, 46)], 'The original pipeline results changed.')
        cursor.execute('SELECT total_payment_records,total_bank_records,valid_records,total_validation_errors FROM batch_validation.dbt_validation_summary')
        require(cursor.fetchall() == [(5, 4, 2, 3)], 'The selected batch views changed unexpectedly.')
    broken = fixtures/'03_broken_format'
    result = subprocess.run([sys.executable, str(ROOT/'run_batch_pipeline.py'),
        '--payment-csv', str(broken/'payments.csv'), '--bank-csv', str(broken/'bank.csv')],
        cwd=ROOT, capture_output=True, text=True)
    require(result.returncode == 1 and 'No database changes.' in result.stderr,
            'Malformed input was not rejected with the expected failure status.')
    require(snapshot(connection) == before, 'Malformed input changed stored data or results.')
    print('PostgreSQL acceptance checks passed: repeat detection, isolated snapshots, saved results and malformed-input rejection.')


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise SystemExit('This exact-count check runs only in the disposable GitHub Actions database.')
    import psycopg2
    from dotenv import dotenv_values
    settings = dotenv_values(ROOT/'.env')
    require(settings.get('DB_NAME') == 'portfolio_validation', 'Unexpected database name.')
    connection = psycopg2.connect(host=settings['DB_HOST'], port=settings['DB_PORT'],
        dbname=settings['DB_NAME'], user=settings['DB_USER'], password=settings['DB_PASSWORD'])
    try:
        connection.set_session(readonly=True, autocommit=True)
        verify(connection)
    finally:
        connection.close()


if __name__ == '__main__':
    main()
