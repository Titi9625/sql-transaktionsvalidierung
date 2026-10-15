"""Read the supported paired CSV format without connecting to a database.

These are format checks. Business findings (including duplicate provider IDs,
pending payments, refunds, missing amounts and non-EUR currencies) are left
for SQL validation. Payment amounts are exact integer minor units; bank
amounts are exact decimal major units. Source-row order is preserved.
"""

import csv
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from io import StringIO
from pathlib import Path
import re


PAYMENT_COLUMNS = (
    'provider_transaction_id', 'created', 'currency', 'amount', 'fee', 'net',
    'status', 'type', 'reporting_category', 'source',
)
BANK_COLUMNS = (
    'bank_transaction_id', 'booking_date', 'reference_text', 'amount',
    'currency', 'counterparty',
)


class CsvFormatError(ValueError):
    """A source file cannot be interpreted using the supported CSV contract."""


@dataclass
class CsvDataset:
    path: Path
    columns: tuple
    rows: list


def _error(path, row_number, column, message):
    location = f'{path.name}: data row {row_number}, column {column}'
    return CsvFormatError(f'{location}: {message}')


def _money(value, path, row_number, column, kind):
    if value == '':
        return None
    if kind == 'payment':
        if not re.fullmatch(r'[+-]?[0-9]+', value):
            raise _error(path, row_number, column,
                         'expected whole cents, for example 4900; no decimal rounding is allowed')
        # PostgreSQL INTEGER is signed 32-bit. Avoid int(float(...)) truncation.
        digits = value.lstrip('+-').lstrip('0') or '0'
        if len(digits) > 10:
            raise _error(path, row_number, column, 'amount exceeds the PostgreSQL INTEGER range')
        amount = int(('-' if value.startswith('-') else '') + digits)
        if not -2147483648 <= amount <= 2147483647:
            raise _error(path, row_number, column, 'amount exceeds the PostgreSQL INTEGER range')
        return amount
    if not re.fullmatch(r'[+-]?[0-9]+(?:\.[0-9]{1,2})?', value):
        raise _error(path, row_number, column,
                     'expected a numeric amount with at most two decimals, for example 49.00')
    amount = Decimal(value)
    if not Decimal('-9999999999.99') <= amount <= Decimal('9999999999.99'):
        raise _error(path, row_number, column, 'amount exceeds the PostgreSQL NUMERIC(12,2) range')
    return amount


def read_csv_dataset(path, kind, *, content=None):
    """Return typed rows; reject unreadable formats, preserve business problems.

    Headers can be reordered, but all and only the supported columns must be
    present. UTF-8 (with or without BOM) and comma-separated quoted fields are
    supported. Empty amounts become None. Empty optional text becomes None.
    IDs, dates, currencies and provider status/type must be supplied.
    Optional content bytes let the batch identifier validate the exact bytes
    that it fingerprints, without reading a changing source file twice.
    """
    if kind not in {'payment', 'bank'}:
        raise ValueError('kind must be payment or bank')
    path = Path(path).expanduser().resolve()
    columns = PAYMENT_COLUMNS if kind == 'payment' else BANK_COLUMNS
    date_column = 'created' if kind == 'payment' else 'booking_date'
    id_column = columns[0]
    required_text = {id_column, 'currency'}
    if kind == 'payment':
        required_text.update({'status', 'type'})
    money_columns = {'amount', 'fee', 'net'} if kind == 'payment' else {'amount'}
    rows = []
    try:
        if content is None:
            content = path.read_bytes()
        with StringIO(content.decode('utf-8-sig'), newline='') as handle:
            reader = csv.DictReader(handle, strict=True)
            header = reader.fieldnames
            if not header:
                raise CsvFormatError(f'{path.name}: missing CSV header')
            if len(header) != len(set(header)):
                raise CsvFormatError(f'{path.name}: duplicate column names in header')
            missing, extra = set(columns) - set(header), set(header) - set(columns)
            if missing or extra:
                detail = []
                if missing:
                    detail.append('missing columns: ' + ', '.join(sorted(missing)))
                if extra:
                    detail.append('unexpected columns: ' + ', '.join(sorted(extra)))
                raise CsvFormatError(f'{path.name}: ' + '; '.join(detail))
            for row_number, raw in enumerate(reader, 1):
                if None in raw or any(value is None for value in raw.values()):
                    raise CsvFormatError(f'{path.name}: data row {row_number}: wrong number of fields')
                row = {column: raw[column].strip() for column in columns}
                for column in required_text:
                    if row[column] == '':
                        raise _error(path, row_number, column, 'required value is empty')
                value = row[date_column]
                if not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
                    raise _error(path, row_number, date_column, 'expected a date in YYYY-MM-DD format')
                try:
                    row[date_column] = date.fromisoformat(value)
                except ValueError as error:
                    raise _error(path, row_number, date_column, 'date does not exist') from error
                for column in money_columns:
                    row[column] = _money(row[column], path, row_number, column, kind)
                for column in set(columns) - money_columns - {date_column}:
                    if row[column] == '':
                        row[column] = None
                rows.append(row)
    except (OSError, UnicodeError, csv.Error) as error:
        raise CsvFormatError(f'{path.name}: cannot read the CSV ({type(error).__name__})') from error
    if not rows:
        raise CsvFormatError(f'{path.name}: no data rows; empty snapshots are not supported yet')
    return CsvDataset(path, columns, rows)


def read_input_pair(payment_csv, bank_csv):
    """Read both selected files. No database connection or writes occur here."""
    return (read_csv_dataset(payment_csv, 'payment'),
            read_csv_dataset(bank_csv, 'bank'))
