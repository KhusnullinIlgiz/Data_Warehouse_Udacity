import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function is used for loading raw data from JSON files to staging tables.
    Input params: cur(connection cursor of DB), conn(connection to the DB)
    Ouput params: 
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function is used for inserting data to data from staging tables.
    Input params: cur(connection cursor of DB), conn(connection to the DB)
    Ouput params: 
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is used for creating connection and cursor of DB and executing load_staging_tables, insert_tables functions.
    Input params:
    Ouput params: 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()