"""Identify exact file pairs and register metadata, without loading raw rows."""

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from .csv_contract import CsvDataset, CsvFormatError, read_csv_dataset


CONTRACT_VERSION = 'csv_pair_v1'


@dataclass
class PreparedPair:
    payment: CsvDataset
    bank: CsvDataset
    payment_fingerprint: str
    bank_fingerprint: str
    pair_fingerprint: str
    contract_version: str = CONTRACT_VERSION


@dataclass
class BatchRegistration:
    batch_id: str
    status: str
    created: bool


def pair_fingerprint(payment_fingerprint, bank_fingerprint, contract_version=CONTRACT_VERSION):
    """Keep source roles and contract version in the pair identity; omit filenames."""
    identity = {'payment':payment_fingerprint, 'bank':bank_fingerprint, 'contract':contract_version}
    content = json.dumps(identity, sort_keys=True, separators=(',',':')).encode('utf-8')
    return hashlib.sha256(content).hexdigest()


def prepare_pair(payment_csv, bank_csv):
    """Capture both files once, then validate and fingerprint those exact bytes."""
    payment_path = Path(payment_csv).expanduser().resolve()
    bank_path = Path(bank_csv).expanduser().resolve()
    def capture(path):
        try:
            return path.read_bytes()
        except OSError as error:
            raise CsvFormatError(f'{path.name}: cannot read the CSV ({type(error).__name__})') from error
    payment_bytes, bank_bytes = capture(payment_path), capture(bank_path)
    payment = read_csv_dataset(payment_path, 'payment', content=payment_bytes)
    bank = read_csv_dataset(bank_path, 'bank', content=bank_bytes)
    payment_hash = hashlib.sha256(payment_bytes).hexdigest()
    bank_hash = hashlib.sha256(bank_bytes).hexdigest()
    return PreparedPair(payment, bank, payment_hash, bank_hash,
                        pair_fingerprint(payment_hash, bank_hash))


def register_batch(connection, pair):
    """Insert one metadata row or return the existing ID, without committing.

    The database unique constraint is the duplicate protection, including
    concurrent registrations. A recognised registered/failed pair is not a
    completed import. The later loader must inspect status and retry as needed.
    """
    with connection.cursor() as cursor:
        cursor.execute('''
            INSERT INTO public.ingestion_batches (
                batch_id, contract_version, pair_fingerprint,
                payment_fingerprint, bank_fingerprint,
                payment_file_name, bank_file_name,
                payment_row_count, bank_row_count
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (pair_fingerprint) DO NOTHING
            RETURNING batch_id, status
        ''', (str(uuid4()), pair.contract_version, pair.pair_fingerprint,
              pair.payment_fingerprint, pair.bank_fingerprint,
              pair.payment.path.name, pair.bank.path.name,
              len(pair.payment.rows), len(pair.bank.rows)))
        inserted = cursor.fetchone()
        if inserted:
            return BatchRegistration(str(inserted[0]), inserted[1], True)
        cursor.execute('''
            SELECT batch_id, status FROM public.ingestion_batches
            WHERE pair_fingerprint = %s
        ''', (pair.pair_fingerprint,))
        existing = cursor.fetchone()
        if existing is None:
            raise RuntimeError('The existing batch could not be read; retry registration.')
        return BatchRegistration(str(existing[0]), existing[1], False)
