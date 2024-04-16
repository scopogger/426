import os
import logging
import datetime
import oracledb
import psycopg
# import multiprocessing as mp
from config import get_config


# Set up logging
def setup_logging():
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

# Database connection functions
def get_postgres_connection(host, port, username, password, database):
    connection_string = f"host={host} port={port} user={username} password={password} dbname={database}"
    return psycopg.connect(connection_string)

def get_oracle_connection(host, port, username, password, service_name):
    connection_string = f"{username}/{password}@{host}:{port}/{service_name}"
    return oracledb.connect(connection_string)

# Data transfer function
def transfer_data(postgres_connection, oracle_connection, select_from_table, insert_into_table):
    with postgres_connection.cursor() as pg_cursor, oracle_connection.cursor() as ora_cursor:
        logging.info(f"Transferring data from {select_from_table} to {insert_into_table}...")

        # Retrieve column names from PostgreSQL table
        pg_cursor.execute(f"SELECT * FROM {select_from_table} LIMIT 1")
        column_names = [col[0] for col in pg_cursor.description]

        # Construct the SQL INSERT statement for Oracle using respective column names
        insert_query = f"INSERT INTO {insert_into_table} ({', '.join(column_names)}) VALUES ({', '.join([':' + col for col in column_names])})"
        ora_cursor.prepare(insert_query)

        # Transfer data row by row
        processed_records = 0
        for row in pg_cursor:
            try:
                ora_cursor.execute(None, {col: val for col, val in zip(column_names, row)})
            except oracledb.DatabaseError as ora_ex:
                if ora_ex.args[0].code != 1:
                    logging.error(f"Error occurred while executing Oracle INSERT queries:\n  {ora_ex}")
            finally:
                processed_records += 1
                logging.info(f"Executed {processed_records} INSERT queries!")


# Main function
def main():
    setup_logging()
    config = get_config()

    for i, postgres_params in enumerate(config["postgres_connection_params"]):
        with get_postgres_connection(**postgres_params) as postgres_conn, \
             get_oracle_connection(**config["oracle_connection_params"]) as oracle_conn:
            for select_table, insert_table in zip(config["select_table_names"], config["insert_table_names"]):
                try:
                    transfer_data(postgres_conn, oracle_conn, f"LMAO.{select_table}", f"{config['schema_names'][i]}.{insert_table}")
                except (psycopg.Error, oracledb.DatabaseError) as error:
                    logging.error(f"Error occurred during data transfer from {select_table} to {insert_table}:\n   {error}")
                
    logging.info("Data transfer completed.")

if __name__ == "__main__":
    main()



# config.py
def get_config():
    postgres_connection_params = [
        {"host": "kn-sigma-db", "port": 5432, "user": "OUT_LMAO", "password": "admin", "dbname": "db_kn"},
        {"host": "ln-sigma-db", "port": 5432, "user": "OUT_LMAO", "password": "admin", "dbname": "db_ln"},
        {"host": "sn-sigma-db", "port": 5432, "user": "OUT_LMAO", "password": "admin", "dbname": "db_sn"},
        {"host": "tn-sigma-db", "port": 5432, "user": "OUT_LMAO", "password": "admin", "dbname": "db_tn"},
        {"host": "fn-sigma-db", "port": 5432, "user": "OUT_LMAO", "password": "admin", "dbname": "db_fn"},
        {"host": "bn-sigma-db", "port": 5432, "user": "OUT_LMAO", "password": "admin", "dbname": "db_bn"},
        {"host": "nsn-sigma-db", "port": 5432, "user": "OUT_LMAO", "password": "admin", "dbname": "db_nsn"},
    ]

    oracle_connection_params = {
        "host": "GIS-CENTR",
        "port": "1521",
        "user": "IN_SIGMA",
        "password": "admin",
        "service_name": "LMAO",
    }

    select_table_names = [
        "LMAO_WELL",
        "LMAO_MEASUREMENT()",
        "LMAO_POINT()",
        "LMAO_DATA()",
        "SP_CODE_INGIB()",
    ]

    insert_table_names = [
        "XTRNL_SIGMA_WELL",
        "XTRNL_SIGMA_MEASUREMENT",
        "XTRNL_SIGMA_POINT",
        "XTRNL_SIGMA_DATA",
        "XTRNL_SIGMA_SP_CODE_INGIB",
    ]

    # Database schema name constants
    schema_names = [
        "LMAOKN",
        "LMAOLN",
        "LMAOSN",
        "LMAOTN",
        "LMAOFN",
        "LMAOBN",
        "LMAONSN",
    ]

    return {
        "postgres_connection_params": postgres_connection_params,
        "oracle_connection_params": oracle_connection_params,
        "select_table_names": select_table_names,
        "insert_table_names": insert_table_names,
        "schema_names": schema_names,
    }