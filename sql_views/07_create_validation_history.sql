CREATE TABLE IF NOT EXISTS public.validation_runs (
    validation_run_id uuid PRIMARY KEY,
    batch_id uuid NOT NULL REFERENCES public.ingestion_batches(batch_id),
    code_version text NOT NULL CHECK (code_version ~ '^[0-9a-f]{64}$'),
    git_commit text,
    git_dirty boolean,
    dbt_invocation_id text NOT NULL,
    dbt_version text NOT NULL,
    dbt_models_built integer NOT NULL CHECK (dbt_models_built > 0),
    dbt_tests_passed integer NOT NULL CHECK (dbt_tests_passed > 0),
    variables jsonb NOT NULL,
    validated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS public.validation_summaries (
    validation_run_id uuid PRIMARY KEY REFERENCES public.validation_runs(validation_run_id),
    validation_date date NOT NULL,
    total_payment_records integer NOT NULL CHECK (total_payment_records >= 0),
    total_bank_records integer NOT NULL CHECK (total_bank_records >= 0),
    valid_records integer NOT NULL CHECK (valid_records >= 0),
    invalid_payment_transactions integer NOT NULL CHECK (invalid_payment_transactions >= 0),
    total_validation_errors integer NOT NULL CHECK (total_validation_errors >= 0),
    duplicate_errors integer NOT NULL CHECK (duplicate_errors >= 0),
    missing_value_errors integer NOT NULL CHECK (missing_value_errors >= 0),
    currency_errors integer NOT NULL CHECK (currency_errors >= 0),
    amount_mismatch_errors integer NOT NULL CHECK (amount_mismatch_errors >= 0),
    unmatched_bank_transactions integer NOT NULL CHECK (unmatched_bank_transactions >= 0),
    bank_reference_id_not_extractable_errors integer NOT NULL CHECK (bank_reference_id_not_extractable_errors >= 0)
);

CREATE TABLE IF NOT EXISTS public.validation_valid_payments (
    validation_run_id uuid NOT NULL REFERENCES public.validation_runs(validation_run_id),
    transaction_id text NOT NULL,
    transaction_date date NOT NULL,
    amount_gross numeric NOT NULL,
    fee_amount numeric,
    amount_net numeric NOT NULL,
    bank_amount numeric NOT NULL,
    currency text NOT NULL,
    status text NOT NULL,
    validation_status text NOT NULL,
    PRIMARY KEY (validation_run_id, transaction_id)
);

CREATE TABLE IF NOT EXISTS public.validation_findings (
    validation_run_id uuid NOT NULL REFERENCES public.validation_runs(validation_run_id),
    finding_number bigint NOT NULL CHECK (finding_number > 0),
    transaction_id text NOT NULL,
    record_source text NOT NULL,
    error_code text NOT NULL,
    error_description text NOT NULL,
    error_field text NOT NULL,
    detected_at timestamptz NOT NULL,
    PRIMARY KEY (validation_run_id, finding_number)
);

CREATE TABLE IF NOT EXISTS public.validation_daily_reports (
    validation_run_id uuid NOT NULL REFERENCES public.validation_runs(validation_run_id),
    reporting_key text NOT NULL,
    payment_date date NOT NULL,
    source_system text NOT NULL,
    payment_row_count bigint NOT NULL,
    distinct_payment_count bigint NOT NULL,
    valid_payment_count bigint NOT NULL,
    flagged_payment_count bigint NOT NULL,
    payment_error_row_count bigint NOT NULL,
    duplicate_payment_id_count bigint NOT NULL,
    validation_pass_rate_pct numeric NOT NULL,
    valid_net_amount_eur numeric NOT NULL,
    PRIMARY KEY (validation_run_id, reporting_key)
);

CREATE INDEX IF NOT EXISTS validation_runs_batch_time_idx
    ON public.validation_runs (batch_id, validated_at);
CREATE INDEX IF NOT EXISTS validation_findings_run_error_idx
    ON public.validation_findings (validation_run_id, error_code);
