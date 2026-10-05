def start_import(connection, file_name, target_table):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO public.import_runs (file_name, target_table)
            VALUES (%s, %s)
            RETURNING run_id;
            """,
            (file_name, target_table),
        )
        run_id = cursor.fetchone()[0]

    connection.commit()
    return run_id


def finish_import(connection, run_id, rows_imported):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE public.import_runs
            SET status = 'success',
                finished_at = clock_timestamp(),
                rows_imported = %s
            WHERE run_id = %s;
            """,
            (rows_imported, run_id),
        )


def fail_import(connection, run_id, error):
    connection.rollback()

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE public.import_runs
            SET status = 'failed',
                finished_at = clock_timestamp(),
                rows_imported = 0,
                error_message = %s
            WHERE run_id = %s;
            """,
            (type(error).__name__, run_id),
        )

    connection.commit()