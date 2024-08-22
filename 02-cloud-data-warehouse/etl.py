import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from logging_config import logging


def load_staging_tables(cur, conn):
    """Load data into staging tables from S3."""
    logging.info("Starting to load data into staging tables.")
    try:
        for query in copy_table_queries:
            logging.info(f"Running query: {query}")
            cur.execute(query)
            conn.commit()
        logging.info("Staging tables loaded successfully.")
    except Exception as e:
        logging.error(f"Error loading staging tables: {e}")
        conn.rollback()

def insert_tables(cur, conn):
    """Insert data from staging tables into the STAR Schema tables."""
    logging.info("Starting to insert data into STAR Schema tables.")
    try:
        for query in insert_table_queries:
            logging.info(f"Running query: {query}")
            cur.execute(query)
            conn.commit()
        logging.info("Data inserted into STAR Schema tables successfully.")
    except Exception as e:
        logging.error(f"Error inserting data into STAR Schema tables: {e}")
        conn.rollback()


def main():
    """Connect to the Redshift cluster, load staging tables, insert into STAR Schema tables, and close the connection."""
    logging.info("Starting ETL process.")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect(
            user=config.get("DWH", "DWH_DB_USER"),
            password=config.get("DWH", "DWH_DB_PASSWORD"),
            host=config.get("DWH", "DWH_ENDPOINT"),
            port=config.get("DWH", "DWH_PORT"),
            dbname=config.get("DWH", "DWH_DB"),
        )
        cur = conn.cursor()
        logging.info("Database connection established.")

        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        cur.close()
        conn.close()
        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error(f"Error during ETL process: {e}")


if __name__ == "__main__":
    main()