#import libraries
import psycopg2 as ps
from sql_queries import drop_table_queries, create_table_queries


def create_database():
    '''Create and connect to databas, Return cursor and connection to DB'''
    # connect to default database
    conn = ps.connect("host=127.0.0.1 user=postgres password=postgre")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create Business DataBase With UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS businesslocation")
    cur.execute("CREATE DATABASE businesslocation WITH ENCODING 'utf8'")

    # close connection to Default database
    cur.close()

    # connect to businessLocation DataBase
    conn = ps.connect("host=127.0.0.1 dbname=businesslocation  user=postgres password=postgre")
    cur = conn.cursor()

    return  cur, conn


def drop_tables(cur, conn):
    # drop all tables created on database
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    # create all tablles
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """ Function to drop and re create businessLocation database and all related tables.
        Usage: python create_tables.py
    """
    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()