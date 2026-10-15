"""Load captured, validated file pairs into append-only batch history tables."""

from dataclasses import dataclass
import sys

from .batch_registry import register_batch
from .csv_contract import BANK_COLUMNS, PAYMENT_COLUMNS


class BatchStateError(RuntimeError):
    """Registry status or history counts disagree; do not repair by deleting data."""


@dataclass
class BatchLoadResult:
    batch_id: str
    status: str
    payment_rows_inserted: int
    bank_rows_inserted: int
    skipped: bool


def _history_counts(cursor, batch_id):
    cursor.execute('''SELECT
        (SELECT count(*) FROM public.raw_payment_history WHERE batch_id = %s),
        (SELECT count(*) FROM public.raw_bank_history WHERE batch_id = %s)
    ''', (batch_id,batch_id))
    return tuple(cursor.fetchone())


def _insert_payment_history(cursor, batch_id, dataset):
    cursor.executemany('''INSERT INTO public.raw_payment_history (
        batch_id, source_row_number, provider_transaction_id, created,
        currency, amount, fee, net, status, type, reporting_category, source
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
        ((batch_id,number,*(row[column] for column in PAYMENT_COLUMNS))
         for number,row in enumerate(dataset.rows,1)))


def _insert_bank_history(cursor, batch_id, dataset):
    cursor.executemany('''INSERT INTO public.raw_bank_history (
        batch_id, source_row_number, bank_transaction_id, booking_date,
        reference_text, amount, currency, counterparty
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
        ((batch_id,number,*(row[column] for column in BANK_COLUMNS))
         for number,row in enumerate(dataset.rows,1)))


def load_batch(connection, pair):
    """Own transactions on a fresh connection dedicated to this operation.

    Registration commits first so failed imports can retain batch metadata.
    A row lock serialises loaders for this batch. Both history inserts and the
    ready transition commit together; an exception rolls them all back.
    The caller closes the connection. No existing raw table is touched.
    """
    with connection:
        registration = register_batch(connection,pair)
    batch_id = registration.batch_id
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute('''SELECT status, payment_row_count, bank_row_count
                    FROM public.ingestion_batches WHERE batch_id = %s FOR UPDATE''',
                    (batch_id,))
                metadata = cursor.fetchone()
                if metadata is None:
                    raise BatchStateError('Registered batch was not found.')
                status, payment_count, bank_count = metadata
                expected = (len(pair.payment.rows),len(pair.bank.rows))
                if (payment_count,bank_count) != expected:
                    raise BatchStateError('Registry row counts disagree with the selected pair.')
                existing = _history_counts(cursor,batch_id)
                if status == 'ready':
                    if existing != expected:
                        raise BatchStateError('Ready batch history counts disagree with its registry.')
                    return BatchLoadResult(batch_id,'ready',0,0,True)
                if existing != (0,0):
                    raise BatchStateError('An incomplete batch already contains history rows.')
                cursor.execute('''UPDATE public.ingestion_batches
                    SET status = 'loading', load_started_at = clock_timestamp(),
                        loaded_at = NULL, last_error_type = NULL
                    WHERE batch_id = %s''',(batch_id,))
                _insert_payment_history(cursor,batch_id,pair.payment)
                _insert_bank_history(cursor,batch_id,pair.bank)
                if _history_counts(cursor,batch_id) != expected:
                    raise BatchStateError('Loaded history counts disagree with the selected pair.')
                cursor.execute('''UPDATE public.ingestion_batches
                    SET status = 'ready', loaded_at = clock_timestamp(),
                        last_error_type = NULL WHERE batch_id = %s''',(batch_id,))
        return BatchLoadResult(batch_id,'ready',expected[0],expected[1],False)
    except Exception as error:
        # The history transaction has already rolled back. Save failure metadata
        # separately, without downgrading a batch another loader made ready.
        try:
            with connection:
                with connection.cursor() as cursor:
                    cursor.execute('''UPDATE public.ingestion_batches
                        SET status = 'failed', last_error_type = %s
                        WHERE batch_id = %s AND status <> 'ready' ''',
                        (type(error).__name__,batch_id))
        except Exception:
            print('Could not save failure metadata; the original load error is preserved.',file=sys.stderr)
        raise
