-- File-pair metadata only. Raw transaction tables remain unchanged.
CREATE TABLE IF NOT EXISTS public.ingestion_batches (
    batch_id uuid PRIMARY KEY,
    contract_version text NOT NULL,
    pair_fingerprint text NOT NULL UNIQUE
        CHECK (pair_fingerprint ~ '^[0-9a-f]{64}$'),
    payment_fingerprint text NOT NULL
        CHECK (payment_fingerprint ~ '^[0-9a-f]{64}$'),
    bank_fingerprint text NOT NULL
        CHECK (bank_fingerprint ~ '^[0-9a-f]{64}$'),
    payment_file_name text NOT NULL,
    bank_file_name text NOT NULL,
    payment_row_count integer NOT NULL CHECK (payment_row_count > 0),
    bank_row_count integer NOT NULL CHECK (bank_row_count > 0),
    status text NOT NULL DEFAULT 'registered'
        CHECK (status IN ('registered', 'loading', 'ready', 'failed')),
    registered_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);
