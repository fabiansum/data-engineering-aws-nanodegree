import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from logging_config import logging


def drop_tables(cur, conn):
    """Drop all tables in the database."""
    logging.info("Dropping existing tables if any.")    
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        logging.info("Tables dropped successfully.")
    except Exception as e:
        logging.error(f"Error dropping tables: {e}")
        conn.rollback()


def create_tables(cur, conn):
    """Create all tables in the database."""
    logging.info("Creating tables.")
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        logging.info("Tables created successfully.")
    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        conn.rollback()
        
def main():
    """Main function to drop and create tables."""
    # Read configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish database connection
    try:
        conn = psycopg2.connect(
            user=config.get("DWH", "DWH_DB_USER"),
            password=config.get("DWH", "DWH_DB_PASSWORD"),
            host=config.get("DWH", "DWH_ENDPOINT"),
            port=config.get("DWH", "DWH_PORT"),
            dbname=config.get("DWH", "DWH_DB"),
        )
        cur = conn.cursor()
        logging.info("Connected to the database.")

        # Drop and create tables
        drop_tables(cur, conn)
        create_tables(cur, conn)

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            logging.info("Database connection closed.")
        else:
            pass


if __name__ == "__main__":
    main()