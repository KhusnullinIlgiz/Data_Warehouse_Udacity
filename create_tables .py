import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function is used for dropping created tables in DB
    Input params: cur(connection cursor of DB), conn(connection to the DB)
    Ouput params: 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Description: This function is used for creating tables in DB
        Input params: cur(connection cursor of DB), conn(connection to the DB)
        Ouput params: 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is used for creating connection and cursor of DB and executing drop_tables, create_tables functions.
    Input params:
    Ouput params: 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()