ALTER TABLE public.ingestion_batches
    ADD COLUMN IF NOT EXISTS load_started_at timestamptz,
    ADD COLUMN IF NOT EXISTS loaded_at timestamptz,
    ADD COLUMN IF NOT EXISTS last_error_type text;

CREATE TABLE IF NOT EXISTS public.raw_payment_history (
    batch_id uuid NOT NULL REFERENCES public.ingestion_batches(batch_id),
    source_row_number integer NOT NULL CHECK (source_row_number > 0),
    provider_transaction_id text NOT NULL,
    created date NOT NULL,
    currency text NOT NULL,
    amount integer,
    fee integer,
    net integer,
    status text NOT NULL,
    type text NOT NULL,
    reporting_category text,
    source text,
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_id, source_row_number)
);

CREATE TABLE IF NOT EXISTS public.raw_bank_history (
    batch_id uuid NOT NULL REFERENCES public.ingestion_batches(batch_id),
    source_row_number integer NOT NULL CHECK (source_row_number > 0),
    bank_transaction_id text NOT NULL,
    booking_date date NOT NULL,
    reference_text text,
    amount numeric(12,2),
    currency text NOT NULL,
    counterparty text,
    imported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (batch_id, source_row_number)
);

CREATE INDEX IF NOT EXISTS raw_payment_history_batch_transaction_idx
    ON public.raw_payment_history (batch_id, provider_transaction_id);
CREATE INDEX IF NOT EXISTS raw_bank_history_batch_booking_idx
    ON public.raw_bank_history (batch_id, bank_transaction_id);
