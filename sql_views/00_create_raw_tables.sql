-- Table: public.raw_payment_transactions

-- DROP TABLE IF EXISTS public.raw_payment_transactions;

CREATE TABLE IF NOT EXISTS public.raw_payment_transactions
(
    provider_transaction_id text COLLATE pg_catalog."default",
    created date,
    currency text COLLATE pg_catalog."default",
    amount integer,
    fee integer,
    net integer,
    status text COLLATE pg_catalog."default",
    type text COLLATE pg_catalog."default",
    reporting_category text COLLATE pg_catalog."default",
    source text COLLATE pg_catalog."default",
    imported_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.raw_payment_transactions
    OWNER to postgres;


-- Table: public.raw_bank_transactions

-- DROP TABLE IF EXISTS public.raw_bank_transactions;

CREATE TABLE IF NOT EXISTS public.raw_bank_transactions
(
    bank_transaction_id text COLLATE pg_catalog."default",
    booking_date date,
    reference_text text COLLATE pg_catalog."default",
    amount numeric(12,2),
    currency text COLLATE pg_catalog."default",
    counterparty text COLLATE pg_catalog."default",
    imported_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.raw_bank_transactions
    OWNER to postgres;


-- Table: public.raw_stripe_api_transactions
-- Nur erforderlich fuer den optionalen Stripe-Test-API-Import (Anhang C.3).

-- DROP TABLE IF EXISTS public.raw_stripe_api_transactions;

CREATE TABLE IF NOT EXISTS public.raw_stripe_api_transactions
(
    provider_transaction_id text COLLATE pg_catalog."default" NOT NULL,
    latest_charge_id text COLLATE pg_catalog."default",
    created date,
    currency text COLLATE pg_catalog."default",
    amount integer,
    fee integer,
    net integer,
    status text COLLATE pg_catalog."default",
    type text COLLATE pg_catalog."default",
    reporting_category text COLLATE pg_catalog."default",
    source text COLLATE pg_catalog."default",
    imported_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT raw_stripe_api_transactions_pkey PRIMARY KEY (provider_transaction_id)
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.raw_stripe_api_transactions
    OWNER to postgres;
