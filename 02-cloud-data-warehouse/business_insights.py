import configparser
import psycopg2
from sql_queries import business_queries


def run_business_queries(cur, conn):
    """Execute business insight queries, fetch results, and print them."""
    for query in business_queries:
        cur.execute(query)
        conn.commit()
        
        # Fetch all results from the executed query
        results = cur.fetchall()
        
        # Print the query results
        print(f"Results for query: {query}")
        for row in results:
            print(row)
        print("\n" + "="*50 + "\n")


def main():
    """Connect to the Redshift cluster, run business queries, and close the connection."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish connection to the Redshift cluster
    conn = psycopg2.connect(
        user=config.get("DWH", "DWH_DB_USER"),
        password=config.get("DWH", "DWH_DB_PASSWORD"),
        host=config.get("DWH", "DWH_ENDPOINT"),
        port=config.get("DWH", "DWH_PORT"),
        dbname=config.get("DWH", "DWH_DB"),
    )
    cur = conn.cursor()

    # Run the business insight queries
    run_business_queries(cur, conn)

    # Close the cursor and connection
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
