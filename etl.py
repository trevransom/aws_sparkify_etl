import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3
import pandas as pd
from tqdm import tqdm


def load_staging_tables(cur, conn):
    """
    - Here we are going load the song file into a dataframe
    - Then we insert it to the relevant table if it contains just 1 entry
    """
    for query in tqdm(copy_table_queries, ascii=True):
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


def insert_tables(cur, conn):
    """
    - Here we are going load the song file into a dataframe
    - Then we insert it to the relevant table if it contains just 1 entry
    """
    for query in tqdm(insert_table_queries, ascii=True):
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print(e)


def main():
    """
    - Connect to database
    - Call data processing functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
