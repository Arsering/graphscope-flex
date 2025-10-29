import os
import glob
import re
import numpy as np
import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib as mpl
from collections import defaultdict

def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            'figure.figsize': (8, 4),
            'savefig.dpi': 300,
            'axes.labelsize': 17,
            'xtick.labelsize': 16,
            'ytick.labelsize': 16,
            'xtick.direction': 'in',
            'ytick.direction': 'in',
            'legend.fontsize': 17
        }
    )
    
def get_thread_number(filename):
    # Extract number from filename using regex
    match = re.search(r'thread_log_(\d+)', filename)
    if match:
        return int(match.group(1))
    return -1

def traverse_thread_logs(log_path):    
    # Check if directory exists
    if not os.path.exists(log_path):
        print(f"Directory does not exist: {log_path}")
        return
    
    # Find all thread log files
    thread_logs = glob.glob(os.path.join(log_path, "*thread_log*"))
    
    # Filter logs with number > 29
    # thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 29]
    
    # Dictionary to store arrays for each thread log
    thread_data = {}
    mean_latencies = np.zeros(21)
    num_queries = np.zeros(21)

    con = duckdb.connect("mydata.db")
    # con.execute("DROP TABLE IF EXISTS mytable")
    # con.execute("""
    #     CREATE TABLE mytable (
    #         a INTEGER,
    #         b INTEGER,
    #         c UBIGINT
    #     )
    # """)
    
    # threshold = 3016243195734026.5
    # for log_file in thread_logs:
    #     filename = os.path.basename(log_file)
    #     thread_num = get_thread_number(filename)
    #     print(f"\nProcessing: {filename} (Thread {thread_num})")
        
    #     con.execute(f"""
    #         INSERT INTO mytable
    #         SELECT 
    #             TRY_CAST(column0 AS INTEGER) AS a,
    #             TRY_CAST(column1 AS INTEGER) AS b,
    #             TRY_CAST(column2 AS UBIGINT) AS c
    #         FROM read_csv_auto('{log_file}', delim=' ', header=false, ignore_errors=True, types={{
    #             'column0':'INTEGER',
    #             'column1':'INTEGER',
    #             'column2':'UBIGINT'
    #         }})
    #         WHERE TRY_CAST(column0 AS INTEGER) IS NOT NULL
    #           AND TRY_CAST(column1 AS INTEGER) IS NOT NULL
    #           AND TRY_CAST(column2 AS UBIGINT) IS NOT NULL
    #           AND TRY_CAST(column2 AS UBIGINT) < {threshold}
    #     """)
    #     # break

        
    # median_value = con.execute("SELECT median(c) FROM mytable").fetchone()[0]
    # print(f"Median value: {median_value}")
    
    # # 按第一列分组，计算第二列的最大值和最小值
    # result = con.execute("""
    #     SELECT 
    #         a,
    #         MAX(b) AS b_max,
    #         MIN(b) AS b_min
    #     FROM mytable
    #     GROUP BY a
    #     ORDER BY a ASC
    # """).fetchdf()
    # print(result)
    
    # id_max = 0
    # for row in result.itertuples(index=False):
    #     con.execute(f"""
    #         UPDATE mytable
    #         SET b = b + {id_max}
    #         WHERE a = {row.a}
    #     """)
    #     id_max = id_max + row.b_max - row.b_min
    #     print(row.a, row.b_max, row.b_min)
    
    # result = con.execute("""
    #     SELECT 
    #         a,
    #         MAX(b) AS b_max,
    #         MIN(b) AS b_min
    #     FROM mytable
    #     GROUP BY a
    #     ORDER BY a ASC
    # """).fetchdf()
    # print(result)
    
    # # 查询第二列
    # b_values = con.execute("SELECT b FROM mytable ORDER BY c ASC").fetchall()

    # # fetchall 返回 list of tuples，转换成单独的 list
    # b_list = [row[0] for row in b_values]


    con.execute("""
        COPY (SELECT b FROM mytable ORDER BY c ASC) 
        TO 'b_column_sorted.csv' (HEADER, DELIMITER ',')
    """)
    
    max_value = con.execute("SELECT MAX(b) FROM mytable").fetchone()[0]
    print(f"max_value = {max_value}")
    
    unique_count = con.execute("SELECT COUNT(DISTINCT b) FROM mytable").fetchone()[0]
    print("第二列去重后的数量:", unique_count)
    result = con.execute("""
        SELECT 
            ((rn - 1) / 100) AS group_id,
            AVG(freq) AS avg_freq
        FROM (
            SELECT 
                b,
                COUNT(*) AS freq,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
            FROM mytable
            GROUP BY b
        )
        GROUP BY group_id
        ORDER BY group_id
    """).fetchdf()
    print(result.head())
    
    # 提取数据
    
    # x = result['group_id']
    # y = result['avg_freq']

    # # 画图
    # plt.figure(figsize=(8, 5))
    # plt.plot(x, y, marker='o', linewidth=1.5, markersize=3)
    # plt.tight_layout()
    # plt.savefig('figure_38.png')
    
def analysis_data(log_path):
    con = duckdb.connect("mydata.db")
    
        
    print("Load eviction log")
    con.execute("DROP TABLE IF EXISTS eviction_log")
    con.execute("""
        CREATE TABLE eviction_log (
            a UBIGINT,
            b UBIGINT
        )
    """)
    thread_logs = glob.glob(os.path.join(log_path, "*thread_log*"))
    thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 0]
    for log_file in thread_logs:
        print(log_file)
        con.execute(f"""
            INSERT INTO eviction_log
            SELECT 
                TRY_CAST(column0 AS UBIGINT) AS a,
                TRY_CAST(column1 AS UBIGINT) AS b,
            FROM read_csv_auto('{log_file}', delim=' ', header=false, ignore_errors=True, types={{
                'column0':'UBIGINT',
                'column1':'UBIGINT',
            }})
            WHERE TRY_CAST(column0 AS UBIGINT) IS NOT NULL
                AND TRY_CAST(column1 AS UBIGINT) IS NOT NULL
        """)

    result = con.execute("""
            SELECT 
                b, 
                AVG(a) AS avg_a  
            FROM eviction_log
            GROUP BY b  
            ORDER BY b  
        """).fetchall()
    print(result)
    
if __name__ == "__main__":
    log_path = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-25-22:27:08/server/graphscope_logs'
    df_1 = analysis_data(log_path)
