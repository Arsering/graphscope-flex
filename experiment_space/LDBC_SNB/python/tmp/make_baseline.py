import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import duckdb

def order_item_by_person_sorted_by_date(conn, person_csv, item_hasCreator_person_csv, item_csv, output_csv, item_id_name):
    types = {"creationDate": "STRING"}

    # 读取 item_table 表并将 creationDate 转换为 TIMESTAMP 类型
    print("Reading item_table")
    conn.execute(f"""
        CREATE TABLE item_table AS 
        SELECT *, 
               TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
        FROM read_csv_auto('{item_csv}', types := ?)
    """, [types])

    # 读取 item_person_table 表
    print("Reading item_person_table")
    conn.execute(f"""
        CREATE TABLE item_person_table AS 
        SELECT *
        FROM read_csv_auto('{item_hasCreator_person_csv}')
    """)

    # 通过 person_table.id 和 item_person_table."Person.id" 进行 join，得到 item_person_order_table 表
    print("Creating item_person_order_table by joining person_table and item_person_table with row number")
    conn.execute(f"""
        CREATE TABLE item_person_order_table AS
        SELECT item_person_table.*, person_table.row_num
        FROM item_person_table
        JOIN person_table
        ON person_table.id = item_person_table."Person.id"
    """)

    # 通过 item_table.id 和 item_person_order_table.{item_id_name} 进行 join，得到 item_with_creator 表
    print("Creating item_with_creator table by joining item_table and item_person_order_table with row number")
    conn.execute(f"""
        CREATE TABLE item_with_creator AS
        SELECT item_table.*, item_person_order_table.row_num
        FROM item_table
        JOIN item_person_order_table
        ON item_table.id = item_person_order_table."{item_id_name}"
    """)

    # 按 row_num 和 creationDateTime 进行排序
    print("Sorting item_with_creator by row_num and creationDateTime")
    conn.execute("""
        CREATE TABLE sorted_item_with_creator AS
        SELECT * FROM item_with_creator
        ORDER BY row_num, creationDateTime
    """)

    # 获取 item_table 中的列（不包含 creationDateTime）
    columns_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'item_table'"
    columns = conn.execute(columns_query).fetchall()
    column_names = [col[0] for col in columns if col[0] != 'creationDateTime']
    columns_str = ', '.join(column_names)

    # 导出结果到 CSV 文件
    print(f"Exporting sorted results to {output_csv}")
    conn.execute(f"""
        COPY (SELECT {columns_str} FROM sorted_item_with_creator) 
        TO '{output_csv}' (DELIMITER '|')
    """)
    
    print(f"Data successfully saved to {output_csv}")
    
    # 清理表
    # conn.execute("DROP TABLE IF EXISTS person_table")
    conn.execute("DROP TABLE IF EXISTS item_table")
    conn.execute("DROP TABLE IF EXISTS item_person_table")
    conn.execute("DROP TABLE IF EXISTS item_person_order_table")
    conn.execute("DROP TABLE IF EXISTS item_with_creator")
    conn.execute("DROP TABLE IF EXISTS sorted_item_with_creator")


def make_baseline(conn,person_csv,forum_csv,comment_csv,post_csv):
    # 读取 person 表并排序
    print("Reading and sorting person table")
    conn.execute(f"""
        CREATE TABLE person_sorted AS 
        SELECT *, 
               TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
        FROM read_csv_auto('{person_csv}', types := ?)
        ORDER BY creationDateTime
    """, [types])

    # 读取 forum 表并排序 
    print("Reading and sorting forum table")
    conn.execute(f"""
        CREATE TABLE forum_sorted AS
        SELECT *,
               TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
        FROM read_csv_auto('{forum_csv}', types := ?)
        ORDER BY creationDateTime
    """, [types])

    # 读取 comment 表并排序
    print("Reading and sorting comment table")
    conn.execute(f"""
        CREATE TABLE comment_sorted AS
        SELECT *,
               TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
        FROM read_csv_auto('{comment_csv}', types := ?)
        ORDER BY creationDateTime
    """, [types])

    # 读取 post 表并排序
    print("Reading and sorting post table")
    conn.execute(f"""
        CREATE TABLE post_sorted AS
        SELECT *,
               TRY_CAST(creationDate AS TIMESTAMP WITH TIME ZONE) AS creationDateTime
        FROM read_csv_auto('{post_csv}', types := ?)
        ORDER BY creationDateTime
    """, [types])

    person_output_csv=OUTPUT_DIR+'person_0_0_baseline.csv'
    forum_output_csv=OUTPUT_DIR+'forum_0_0_baseline.csv'
    comment_output_csv=OUTPUT_DIR+'comment_0_0_baseline.csv'
    post_output_csv=OUTPUT_DIR+'post_0_0_baseline.csv'

    # 导出排序后的数据
    for table, output_file in [
        ('person_sorted', person_output_csv),
        ('forum_sorted', forum_output_csv),
        ('comment_sorted', comment_output_csv),
        ('post_sorted', post_output_csv)
    ]:
        print(f"Exporting sorted {table} to {output_file}")
        
        # 获取表的列名(不包含creationDateTime)
        columns = conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'").fetchall()
        column_names = [col[0] for col in columns if col[0] != 'creationDateTime']
        columns_str = ', '.join(column_names)
        
        # 导出数据
        conn.execute(f"""
            COPY (SELECT {columns_str} FROM {table})
            TO '{output_file}' (DELIMITER '|')
        """)

    # 清理表
    conn.execute("DROP TABLE IF EXISTS person_sorted")
    conn.execute("DROP TABLE IF EXISTS forum_sorted") 
    conn.execute("DROP TABLE IF EXISTS comment_sorted")
    conn.execute("DROP TABLE IF EXISTS post_sorted")

def make_baseline_by_order_message_by_person(conn,person_csv,comment_csv,post_csv):
    # 读取 person_table 表并添加行号
    print("Reading person_table with row number")
    conn.execute(f"""
        CREATE TABLE person_table AS 
        SELECT id, ROW_NUMBER() OVER () AS row_num
        FROM read_csv_auto('{person_csv}', types := ?)
    """, [types])

    # for i in range(0,5):
    cur_dir=INPUT_DIR
    comment_csv=cur_dir+'comment_0_0.csv'
    post_csv=cur_dir+'post_0_0.csv'
    comment_hasCreator_person_csv=cur_dir+'comment_hasCreator_person_0_0.csv'
    post_hasCreator_person_csv=cur_dir+'post_hasCreator_person_0_0.csv'
    new_comment_csv=OUTPUT_DIR+'new_comment_0_0.csv'
    new_post_csv=OUTPUT_DIR+'new_post_0_0.csv'
    order_item_by_person_sorted_by_date(conn,person_csv,comment_hasCreator_person_csv,comment_csv,new_comment_csv,"Comment.id")
    order_item_by_person_sorted_by_date(conn,person_csv,post_hasCreator_person_csv,post_csv,new_post_csv,"Post.id")


conn = duckdb.connect()
conn.execute("SET enable_progress_bar = true")

sf = 30
INPUT_DIR=f'/nvme0n1/00new_db/sf{sf}/social_network/dynamic/'
OUTPUT_DIR=f'/nvme0n1/sf{sf}/'
person_csv=INPUT_DIR+'person_0_0.csv'
person_baseline_csv=OUTPUT_DIR+'person_0_0_baseline.csv'
forum_csv=INPUT_DIR+'forum_0_0.csv'
comment_csv=INPUT_DIR+'comment_0_0.csv'
post_csv=INPUT_DIR+'post_0_0.csv'
types = {"creationDate": "VARCHAR"}
make_baseline(conn,person_csv,forum_csv,comment_csv,post_csv)
make_baseline_by_order_message_by_person(conn,person_baseline_csv,comment_csv,post_csv)