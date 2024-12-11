import csv
from collections import defaultdict
from tqdm import tqdm
import pandas as pd
import duckdb
import numpy as np


def person_create_post_order(post_hasCreator_person_csv, person_vertex_csv):
    conn = duckdb.connect()
    conn.execute("SET enable_progress_bar = true")
    
    query = """
        CREATE TABLE post_hasCreator_person AS 
        SELECT * FROM read_csv_auto(?);
    """
    conn.execute(query, [post_hasCreator_person_csv])
    
    result = conn.execute("""
        SELECT 
            "Person.id", 
            COUNT(*) AS count
        FROM 
            post_hasCreator_person
        GROUP BY 
            "Person.id"
        ORDER BY 
            count ASC;  -- 按行数降序排序
    """).fetchall()
    
    df = pd.read_csv(person_vertex_csv, delimiter='|', low_memory=False)
    id_list = df['id']
    dict_tmp = {}
    index = 0 
    for item in id_list:
        dict_tmp[item] = index
        index += 1
        
    degree_counter = np.zeros(id_list.size)
    
    for row in result:
        degree_counter[dict_tmp[row[0]]] = row[1]
    
    # for row in result:
    #     print(row)
    #     break
    
    return  dict_tmp, degree_counter

def person_knows_person_order(person_knows_person_csv, person_vertex_csv):
    df = pd.read_csv(person_knows_person_csv, sep='|')
    
    # 保存原始列名
    with open(person_knows_person_csv, 'r') as f:
        original_columns = f.readline().strip().split('|')
    print(original_columns)
    
    df.columns = ['start_person_id', 'end_person_id', 'Date']
    # 解决重复列名问题（给重复列名添加后缀）
    print(df.columns.tolist())
    df.to_csv(person_knows_person_csv, sep='|', index=False)

    conn = duckdb.connect()
    conn.execute("SET enable_progress_bar = true")
    
    query = """
        CREATE TABLE person_knows_person AS 
        SELECT * FROM read_csv_auto(?);
    """
    conn.execute(query, [person_knows_person_csv])
    
    result1 = conn.execute("""
        SELECT 
            "start_person_id", 
            COUNT(*) AS count
        FROM 
            person_knows_person
        GROUP BY 
            "start_person_id"
        ORDER BY 
            count ASC;  -- 按行数降序排序
    """).fetchall()
    
    result2 = conn.execute("""
        SELECT 
            "end_person_id", 
            COUNT(*) AS count
        FROM 
            person_knows_person
        GROUP BY 
            "end_person_id"
        ORDER BY 
            count ASC;  -- 按行数降序排序
    """).fetchall()
    
    df = pd.read_csv(person_vertex_csv, delimiter='|', low_memory=False)
    id_list = df['id']
    dict_tmp = {}
    index = 0 
    for item in id_list:
        dict_tmp[item] = index
        index += 1
        
    degree_counter = np.zeros(id_list.size)
    
    for row in result1:
        degree_counter[dict_tmp[row[0]]] += row[1]
    
    for row in result2:
        degree_counter[dict_tmp[row[0]]] += row[1]
        
    df = pd.read_csv(person_knows_person_csv, sep='|')
    
    df.columns = original_columns
    # df.columns = ['Person.id', 'Person.id','CreationDate']

    df.to_csv(person_knows_person_csv, sep='|', index=False)

    return  dict_tmp, degree_counter

if __name__ == "__main__":
    sf_number = 30
    nbr_name = 'comment'
    post_hasCreator_person_csv = f'/nvme0n1/lgraph_db/sf{sf_number}/social_network/dynamic/{nbr_name}_hasCreator_person_0_0.csv'
    person_vertex_csv = f'/nvme0n1/lgraph_db/sf{sf_number}/social_network/dynamic/person_0_0.csv'
    # dict_tmp, degree_counter = person_create_post_order(post_hasCreator_person_csv, person_vertex_csv)
    
    person_knows_person_csv = f'/nvme0n1/lgraph_db/sf{sf_number}/social_network/dynamic/person_knows_person_0_0.csv'
    person_knows_person_order(person_knows_person_csv, person_vertex_csv)
    
    # degree_counter = degree_counter[degree_counter != 0]
    
    # print(sum(degree_counter))
    # print(f'P10:\t{np.percentile(degree_counter, 10)}')
    # print(f'P20:\t{np.percentile(degree_counter, 20)}')
    # print(f'P30:\t{np.percentile(degree_counter, 30)}')
    # print(f'P40:\t{np.percentile(degree_counter, 40)}')
    # print(f'P50:\t{np.percentile(degree_counter, 50)}')
    # print(f'P60:\t{np.percentile(degree_counter, 60)}')
    # print(f'P70:\t{np.percentile(degree_counter, 70)}')
    # print(f'P80:\t{np.percentile(degree_counter, 80)}')
    # print(f'P90:\t{np.percentile(degree_counter, 90)}')
    # print(f'P99:\t{np.percentile(degree_counter, 99)}')
    # print(f'P99:\t{np.percentile(degree_counter, 99)}')

    print('\nwork finished')
