import cx_Oracle
import psycopg2
import logging


def main():
    # Logging configuration (similar to the C# code)
    log_directory = "Logs"
    current_datetime = datetime.datetime.now()
    log_filename = f"{current_datetime:%Y-%m-%d_%H-%M-%S}.log"
    log_filepath = os.path.join(log_directory, log_filename)

    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(filename=log_filepath, level=logging.INFO)

    # Database connection string constants (placeholders for passwords)
    postgres_template = "port=5432 user=out_LMAO dbname={}"
    oracle_template = "user/password@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=GIS-CENTR)(PORT=1521))(CONNECT_DATA=(SID=LMAO)))"

    # Schema names and table names (modify as needed)
    schema_names = [
        "LMAOKN",
        "LMAOLN",
        "LMAOSN",
        "LMAOTN",
        "LMAOFN",
        "LMAOBN",
        "LMAONSN",
    ]
    select_tables = [
        "LMAO_WELL",
        "LMAO_MEASUREMENT()",
        "LMAO_POINT()",
        "LMAO_DATA()",
        "SP_CODE_INGIB()",
    ]
    insert_tables = [
        "XTRNL_sigma_WELL",
        "XTRNL_sigma_MEASUREMENT",
        "XTRNL_sigma_POINT",
        "XTRNL_sigma_DATA",
        "XTRNL_sigma_SP_CODE_INGIB",
    ]

    for schema_name in schema_names:
        try:
            # Construct full connection strings with passwords from environment variables
            postgres_conn_string = postgres_template.format(os.environ.get(f"POSTGRES_PASSWORD_{schema_name}"))
            oracle_conn_string = oracle_template.format(os.environ.get(f"ORACLE_PASSWORD_{schema_name}"))

            data_transfer_master(postgres_conn_string, oracle_conn_string, select_tables, insert_tables, schema_name)
        except (cx_Oracle.DatabaseError, psycopg2.Error) as err:
            logging.error(f"Error occurred during data transfer for schema {schema_name}: {err}")


def data_transfer_master(postgres_conn_string, oracle_conn_string, select_tables, insert_tables, schema_name):
    with psycopg2.connect(postgres_conn_string) as pg_conn, cx_Oracle.connect(oracle_conn_string) as ora_conn:
        logging.info(f"Connection to {schema_name} established! Transferring data...")

        try:
            with pg_conn.cursor() as pg_cur, ora_conn.cursor() as ora_cur:
                for select_table, insert_table in zip(select_tables, insert_tables):
                    full_select_table = f"{schema_name}.{select_table}"
                    logging.info(f"Working on transferring data from {full_select_table} to {insert_table}...")

                    # Retrieve column names from Oracle table
                    pg_cur.execute(f"SELECT * FROM \"{full_select_table}\"")
                    column_names = [desc[0] for desc in pg_cur.description]

                    # Construct the SQL INSERT statement for Oracle using respective column names
                    insert_query = f"INSERT INTO {insert_table} ({','.join(column_names)}) VALUES ({','.join([':' + col for col in column_names])})"
                    ora_cur.prepare(insert_query)

                    # Transfer data row by row
                    processed_records = 0
                    for row in pg_cur:
                        try:
                            ora_cur.execute(None, row)
                            processed_records += 1
                        except cx_Oracle.Error as ora_err:
                            if ora_err.number != 1:  # Ignore ORA-00001: unique constraint violation
                                logging.error(f"Error occured while executing Oracle INSERT queries:\n {ora_err}")
                            else:
                                logging.info(f"Ignoring unique constraint violation for row {processed_records + 1}")

                    logging.info(f"Executed {processed_records} INSERT queries!")

        except (cx_Oracle.DatabaseError, psycopg2.Error) as err:
            logging.error(f"Error during data transfer for schema {schema_name}: {err}")


if __name__ == "__main__":
    main()
