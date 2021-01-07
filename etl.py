import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3
import pandas as pd
from tqdm import tqdm


def load_staging_tables(cur, conn):
    for query in tqdm(copy_table_queries, ascii=True):
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in tqdm(insert_table_queries, ascii=True):
        print(query)
        # this query needs to get passed the data that it'll read in
        # i need to somehow pull the redshift data and clean it and then put in into this cur
        cur.execute(query)
        conn.commit()


def main():
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
