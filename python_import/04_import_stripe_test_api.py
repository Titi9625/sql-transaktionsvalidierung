import os
from datetime import datetime, timezone

import psycopg2
import stripe
from dotenv import load_dotenv


def env_first(names, default=None):
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def obj_get(obj, key, default=None):
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def get_db_connection():
    db_host = env_first(["DB_HOST", "PGHOST"], "localhost")
    db_name = env_first(["DB_NAME", "PGDATABASE"], "bachelorarbeit_validation")
    db_user = env_first(["DB_USER", "PGUSER"], "postgres")
    db_password = env_first(["DB_PASSWORD", "POSTGRES_PASSWORD", "PGPASSWORD"])
    db_port = env_first(["DB_PORT", "PGPORT"], "5432")

    if not db_password:
        raise ValueError(
            "Database password missing. Add DB_PASSWORD=your_postgres_password to the .env file."
        )

    return psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password,
        port=db_port,
    )


def create_table_if_not_exists(conn):
    create_sql = """
    create table if not exists raw_stripe_api_transactions (
        provider_transaction_id text primary key,
        latest_charge_id text,
        created date,
        currency text,
        amount integer,
        fee integer,
        net integer,
        status text,
        type text,
        reporting_category text,
        source text,
        imported_at timestamp without time zone default current_timestamp
    );
    """

    with conn.cursor() as cur:
        cur.execute(create_sql)

    conn.commit()


def create_test_payment_intents_if_needed():
    existing_payment_intents = stripe.PaymentIntent.list(limit=3)

    if len(existing_payment_intents.data) > 0:
        print("Existing Stripe test PaymentIntents found. No new test payments created.")
        return

    print("No Stripe test PaymentIntents found.")
    print("Creating 3 test payments in Stripe test mode...")

    test_amounts = [1990, 2990, 4990]

    for index, amount in enumerate(test_amounts, start=1):
        stripe.PaymentIntent.create(
            amount=amount,
            currency="eur",
            payment_method="pm_card_visa",
            confirm=True,
            payment_method_types=["card"],
            description=f"Bachelorarbeit Stripe API Testzahlung {index}",
            metadata={
                "project": "bachelorarbeit_sql_validierung",
                "test_case": f"api_import_{index}",
            },
        )

    print("3 Stripe test payments created.")


def retrieve_payment_intents():
    payment_intents = stripe.PaymentIntent.list(
        limit=10,
        expand=["data.latest_charge.balance_transaction"],
    )

    return payment_intents.data


def extract_balance_information(payment_intent):
    latest_charge = obj_get(payment_intent, "latest_charge")

    if not latest_charge:
        return None, None, None, "charge"

    if isinstance(latest_charge, str):
        latest_charge = stripe.Charge.retrieve(
            latest_charge,
            expand=["balance_transaction"],
        )

    latest_charge_id = obj_get(latest_charge, "id")
    balance_transaction = obj_get(latest_charge, "balance_transaction")

    if isinstance(balance_transaction, str):
        balance_transaction = stripe.BalanceTransaction.retrieve(balance_transaction)

    fee = obj_get(balance_transaction, "fee")
    net = obj_get(balance_transaction, "net")
    reporting_category = obj_get(balance_transaction, "reporting_category", "charge")

    return latest_charge_id, fee, net, reporting_category


def map_status(payment_intent_status):
    if payment_intent_status == "succeeded":
        return "available"
    return "pending"


def insert_payment_intents(conn, payment_intents):
    insert_sql = """
    insert into raw_stripe_api_transactions (
        provider_transaction_id,
        latest_charge_id,
        created,
        currency,
        amount,
        fee,
        net,
        status,
        type,
        reporting_category,
        source,
        imported_at
    )
    values (
        %(provider_transaction_id)s,
        %(latest_charge_id)s,
        %(created)s,
        %(currency)s,
        %(amount)s,
        %(fee)s,
        %(net)s,
        %(status)s,
        %(type)s,
        %(reporting_category)s,
        %(source)s,
        current_timestamp
    )
    on conflict (provider_transaction_id)
    do update set
        latest_charge_id = excluded.latest_charge_id,
        created = excluded.created,
        currency = excluded.currency,
        amount = excluded.amount,
        fee = excluded.fee,
        net = excluded.net,
        status = excluded.status,
        type = excluded.type,
        reporting_category = excluded.reporting_category,
        source = excluded.source,
        imported_at = current_timestamp;
    """

    rows_inserted_or_updated = 0

    with conn.cursor() as cur:
        for payment_intent in payment_intents:
            payment_intent_id = obj_get(payment_intent, "id")
            created_timestamp = obj_get(payment_intent, "created")

            created_date = datetime.fromtimestamp(
                created_timestamp,
                tz=timezone.utc,
            ).date()

            latest_charge_id, fee, net, reporting_category = extract_balance_information(
                payment_intent
            )

            row = {
                "provider_transaction_id": payment_intent_id,
                "latest_charge_id": latest_charge_id,
                "created": created_date,
                "currency": obj_get(payment_intent, "currency", "").upper(),
                "amount": obj_get(payment_intent, "amount"),
                "fee": fee,
                "net": net,
                "status": map_status(obj_get(payment_intent, "status")),
                "type": "charge",
                "reporting_category": reporting_category,
                "source": latest_charge_id or payment_intent_id,
            }

            cur.execute(insert_sql, row)
            rows_inserted_or_updated += 1

    conn.commit()
    return rows_inserted_or_updated


def main():
    load_dotenv()

    stripe_key = os.getenv("STRIPE_SECRET_KEY")

    if not stripe_key:
        raise ValueError("STRIPE_SECRET_KEY is missing in the .env file.")

    if not stripe_key.startswith("sk_test_"):
        raise ValueError("Use only a Stripe TEST secret key starting with sk_test_.")

    stripe.api_key = stripe_key

    conn = get_db_connection()

    try:
        create_table_if_not_exists(conn)
        create_test_payment_intents_if_needed()

        payment_intents = retrieve_payment_intents()
        rows_inserted_or_updated = insert_payment_intents(conn, payment_intents)

        print("Stripe API import completed successfully.")
        print(f"Stripe PaymentIntents retrieved: {len(payment_intents)}")
        print(
            "Rows inserted or updated in raw_stripe_api_transactions: "
            f"{rows_inserted_or_updated}"
        )

    finally:
        conn.close()


if __name__ == "__main__":
    main()