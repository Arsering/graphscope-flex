import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import duckdb
from datetime import datetime
def append_by_base_file(base_csv, curr_csv, output_csv):
    try:
        # Connect to an in-memory DuckDB instance
        types = {"creationDate": "STRING"}
        conn = duckdb.connect()

        # Load base_csv into table 'a'
        print("read base table")
        conn.execute(f"""
            CREATE TABLE a AS 
            SELECT *, 
                   TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
            FROM read_csv_auto('{base_csv}',types:=?)
        """,[types])

        # Load curr_csv into table 'b'
        print('read curr table')
        conn.execute(f"""
            CREATE TABLE b AS 
            SELECT *, 
                   TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
            FROM read_csv_auto('{curr_csv}',types:=?)
        """,[types])

        # Create table 'c' with rows in 'b' but not in 'a' based on unique identifier (e.g., 'id')
        # Adjust 'id' to the column name that uniquely identifies each row
        print("find differ")
        conn.execute("""
            CREATE TABLE c AS 
            SELECT * 
            FROM b
            WHERE id NOT IN (SELECT id FROM a)
        """)

        # Sort table 'c' by creationDateTime in ascending order
        print('sort differ')
        conn.execute("""
            CREATE TABLE c_sorted AS
            SELECT * 
            FROM c
            ORDER BY creationDateTime ASC
        """)

        # Remove the 'creationDateTime' column from c_sorted
        column_names = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'c_sorted'").fetchall()
        column_names = [col[0] for col in column_names if col[0] != 'creationDateTime']
        columns_str = ', '.join(column_names)

        # Append sorted rows from 'c_sorted' to 'a' and save to output CSV
        print(f'dump to {output_csv}')
        conn.execute(f"""
            COPY (SELECT {columns_str} FROM a
                  UNION ALL
                  SELECT {columns_str} FROM c_sorted)
            TO '{output_csv}' (DELIMITER '|')
        """)

        print(f"Data has been successfully appended and saved to {output_csv}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Clean up tables
        conn.execute("DROP TABLE IF EXISTS a")
        conn.execute("DROP TABLE IF EXISTS b")
        conn.execute("DROP TABLE IF EXISTS c")
        conn.execute("DROP TABLE IF EXISTS c_sorted")

import json

conn = duckdb.connect()
conn.execute("SET enable_progress_bar = true")
PREFIX = '/nvme0n1/sf0.1/'
FULL_PREFIX = '/nvme0n1/lgraph_db/sf0.1/social_network/dynamic/'

person_csv = PREFIX + 'modified_0/person_0_0.csv'
base_comment_csv = PREFIX + 'modified_0/new_comment_0_0.csv'
base_post_csv = PREFIX + 'modified_0/new_post_0_0.csv'
cur_dir = FULL_PREFIX
comment_csv = cur_dir + 'comment_0_0.csv'
post_csv = cur_dir + 'post_0_0.csv'
comment_hasCreator_person_csv=cur_dir+'comment_hasCreator_person_0_0.csv'
post_hasCreator_person_csv=cur_dir+'post_hasCreator_person_0_0.csv'
new_comment_csv=cur_dir+'partial_comment_0_0.csv'
new_post_csv=cur_dir+'partial_post_0_0.csv'
person_list_file=cur_dir+'partial_person_file.csv'
comment_list_file=cur_dir+'partial_comment_file.csv'
post_list_file=cur_dir+'partial_post_file.csv'
append_by_base_file(base_comment_csv,comment_csv,new_comment_csv)
append_by_base_file(base_post_csv,post_csv,new_post_csv)