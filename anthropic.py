import os
import logging
import datetime
import oracledb
import psycopg2

# Set up logging
log_dir = "Logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = f"{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
log_file_path = os.path.join(log_dir, log_filename)

logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Database connection string constants
postgres_connection_string_general = "Port=5432; User Id=out_LMAO; Password=admin "
postgres_connection_strings = [
    "Server=kn-sigma-db; Database=db_kn;",
    "Server=ln-sigma-db; Database=db_ln;",
    "Server=sn-sigma-db; Database=db_sn;",
    "Server=tn-sigma-db; Database=db_tn;",
    "Server=fn-sigma-db; Database=db_fn;",
    "Server=bn-sigma-db; Database=db_bn;",
    "Server=nsn-sigma-db; Database=db_nsn;",
]

oracle_connection_string_general = "Data Source=(DESCRIPTION = (ADDRESS=(PROTOCOL=TCP)(HOST=GIS-CENTR)(PORT=1521)) (CONNECT_DATA = (SID = LMAO)));"
oracle_connection_strings = [
    "User ID=LMAOKN; Password=admin;",
    "User ID=LMAOLN; Password=admin;",
    "User ID=LMAOSN; Password=admin;",
    "User ID=LMAOTN; Password=admin;",
    "User ID=LMAOFN; Password=admin;",
    "User ID=LMAOBN; Password=admin;",
    "User ID=LMAONSN; Password=admin;",
]

select_table_names = [
    "LMAO_WELL",
    "LMAO_MEASUREMENT()",
    "LMAO_POINT()",
    "LMAO_DATA()",
    "SP_CODE_INGIB()",
]
insert_table_names = [
    "XTRNL_sigma_WELL",
    "XTRNL_sigma_MEASUREMENT",
    "XTRNL_sigma_POINT",
    "XTRNL_sigma_DATA",
    "XTRNL_sigma_SP_CODE_INGIB",
]

schema_names = [
    "LMAOKN",
    "LMAOLN",
    "LMAOSN",
    "LMAOTN",
    "LMAOFN",
    "LMAOBN",
    "LMAONSN",
]

def data_transfer_master(postgres_connection_string, oracle_connection_string, select_from_tables, insert_into_tables, schema_name):
    with psycopg2.connect(postgres_connection_string) as pg_conn, oracledb.connect(oracle_connection_string) as ora_conn:
        try:
            logging.info(f"Connection to {schema_name} established! Transferring data...")

            for i, (select_from_table, insert_into_table) in enumerate(zip(select_from_tables, insert_into_tables)):
                construct_and_execute_queries(pg_conn, ora_conn, f"LMAO.{select_from_table}", insert_into_table)
                logging.info(f"Finished transferring data from {select_from_table} to {insert_into_table}")
        except Exception as ex:
            logging.error(f"Error occurred during data transfer: {ex}")

def construct_and_execute_queries(pg_conn, ora_conn, select_from_table, insert_into_table):
    with pg_conn.cursor() as pg_cursor, ora_conn.cursor() as ora_cursor:
        logging.info(f"Working on transferring data from {select_from_table} to {insert_into_table}...")

        # Retrieve column names from PostgreSQL table
        pg_cursor.execute(f"SELECT * FROM {select_from_table}")
        column_names = [col[0] for col in pg_cursor.description]

        # Construct the SQL INSERT statement for Oracle using respective column names
        insert_query = f"INSERT INTO {insert_into_table} ({', '.join(column_names)}) VALUES ({', '.join([':' + col for col in column_names])})"
        ora_cursor.prepare(insert_query)

        # Logging the number of processed INSERT queries
        processed_records = 0

        # Transfer data row by row
        for row in pg_cursor:
            try:
                ora_cursor.execute(None, {col: val for col, val in zip(column_names, row)})
            except oracledb.DatabaseError as ora_ex:
                if ora_ex.args[0].code != 1:
                    logging.error(f"Error occurred while executing Oracle INSERT queries:\n  {ora_ex}")
            finally:
                processed_records += 1
                logging.info(f"Executed {processed_records} INSERT queries!")

if __name__ == "__main__":
    for i, (postgres_connection_string, oracle_connection_string) in enumerate(
        zip(
            [postgres_connection_string_general + conn_str for conn_str in postgres_connection_strings],
            [oracle_connection_string_general + conn_str for conn_str in oracle_connection_strings],
        )
    ):
        data_transfer_master(
            postgres_connection_string,
            oracle_connection_string,
            select_table_names,
            insert_table_names,
            schema_names[i],
        )

    logging.info("Data transfer completed.")