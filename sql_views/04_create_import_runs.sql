CREATE TABLE IF NOT EXISTS public.import_runs (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name text NOT NULL,
    target_table text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'success', 'failed')),
    rows_imported integer NOT NULL DEFAULT 0
        CHECK (rows_imported >= 0),
    error_message text
);