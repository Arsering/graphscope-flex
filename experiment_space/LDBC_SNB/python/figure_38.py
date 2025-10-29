import os
import glob
import re
import numpy as np
import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib as mpl


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
    
def analysis_pingpong(log_path, log_id):
    con = duckdb.connect("mydata.db")
    con.execute("DROP TABLE IF EXISTS pingpong_data")
    con.execute("""
        CREATE TABLE pingpong_data (
            a INTEGER,
            b UBIGINT
        )
    """)
    con.execute(f"""
        INSERT INTO pingpong_data
        SELECT 
            TRY_CAST(column0 AS INTEGER) AS a,
            TRY_CAST(column1 AS UBIGINT) AS b
        FROM read_csv_auto('{log_path}', delim=' ', header=false, ignore_errors=True, types={{
            'column0':'INTEGER',
            'column1':'UBIGINT'
        }})
        WHERE TRY_CAST(column0 AS INTEGER) IS NOT NULL
            AND TRY_CAST(column1 AS UBIGINT) IS NOT NULL
    """)
    
    df = con.execute("""
        SELECT
            mytable.b AS value,
            COUNT(*) AS cnt_t1,
            COALESCE(t2_count.cnt_t2, 0) AS cnt_t2
        FROM mytable
        LEFT JOIN (
            SELECT b, COUNT(*) AS cnt_t2
            FROM pingpong_data
            GROUP BY b
        ) AS t2_count
        ON mytable.b = t2_count.b
        GROUP BY mytable.b, cnt_t2
        ORDER BY cnt_t1 DESC
    """).fetchdf()
    # print(df)
    
    filtered_df = df[df['cnt_t2'] > 1].copy()
    filtered_df['original_row_index'] = filtered_df.index
    print(filtered_df)
    # plot_pingpong(df)

    plot_pingpong(df, log_id)

def plot_pingpong(df, log_id):
    set_mpl()
    colors = ['#0070C0','#8E69B8','#C53A32','#529E3F','#EF8636']

    group_size = 36000
    df = (
        df.groupby(df.index // group_size)[["cnt_t1", "cnt_t2"]]
        .sum()
        .reset_index(drop=True)
    )
    
    # 创建图表和双 y 轴
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()  # 右侧 y 轴
    # 横坐标为行号
    x = df.index
    x = np.array(x) /max(x)*100
    
    # 左 y 轴画 cnt_t1
    ax1.plot(x, df["cnt_t1"], color=colors[0], marker="o")

    # 右 y 轴画 cnt_t2
    # ax2.plot(x, np.array(df["cnt_t2"])/np.array(df["cnt_t1"]), color="tab:red", marker="s", label="K")
    ax2.plot(x, np.array(df["cnt_t2"]), color=colors[1], marker="s")

    # 轴标签
    ax1.set_xlabel("File Page Range (%, Sorted By Access Count)")
    ax1.set_ylabel("Avg. Access Count", color=colors[0])
    ax2.set_ylabel("Avg. Ping-Pong Count", color=colors[1])

    # 刻度颜色
    ax1.tick_params(axis='y', colors=colors[0])
    ax1.ticklabel_format(style='sci', axis='y', scilimits=(0,0))

    ax2.tick_params(axis='y', colors=colors[1])
    ax2.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    ax2.set_ylim(0, 82000)

    # 图例（需要合并两个轴的 legend）
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    # ax1.legend(lines + lines2, labels + labels2, loc="upper right", frameon=False)

    # 网格与标题
    ax1.grid(True, linestyle="--", alpha=0.3)
    # plt.title("cnt_t1 (left) vs cnt_t2 (right)")

    plt.tight_layout()
    plt.savefig(f'figs/Access_vs_PingPong_{log_id}.pdf')
    
def analysis_zipf(trace_data_path, pingpong_data_path):
    con = duckdb.connect("mydata.db")
    
    # load trace data
    con.execute("DROP TABLE IF EXISTS zipf_trace")
    con.execute("""
        CREATE TABLE zipf_trace (   
            a UBIGINT
        )
    """)
    
    con.execute(f"""
        INSERT INTO zipf_trace
        SELECT 
            TRY_CAST(column0 AS UBIGINT) AS a
        FROM read_csv_auto('{trace_data_path}', delim=' ', header=false, ignore_errors=True, types={{
            'column0':'UBIGINT'
        }})
        WHERE TRY_CAST(column0 AS UBIGINT) IS NOT NULL
    """)
    
    # load pingpong data
    con.execute("DROP TABLE IF EXISTS zipf_pingpong")
    con.execute("""
        CREATE TABLE zipf_pingpong (   
            a UBIGINT,
            b UBIGINT
        )
    """)
    
    con.execute(f"""
        INSERT INTO zipf_pingpong
        SELECT 
            TRY_CAST(column0 AS UBIGINT) AS a,
            TRY_CAST(column1 AS UBIGINT) AS b
        FROM read_csv_auto('{pingpong_data_path}', delim=' ', header=false, ignore_errors=True, types={{
            'column0':'UBIGINT'
        }})
        WHERE TRY_CAST(column0 AS UBIGINT) IS NOT NULL
    """)
    
    # # analysis trace data
    # result_trace = con.execute("""
    #     SELECT 
    #         (rn - 1) // 10000 AS group_id,
    #         AVG(freq) AS avg_freq
    #     FROM (
    #         SELECT 
    #             a,
    #             COUNT(*) AS freq,
    #             ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
    #         FROM zipf_trace
    #         GROUP BY a
    #     )
    #     GROUP BY group_id
    #     ORDER BY group_id
    # """).fetchdf()
    # print(result_trace.head())
    
    df = con.execute("""
        SELECT
            zipf_trace.a AS value,
            COUNT(*) AS cnt_t1,
            COALESCE(t2_count.cnt_t2, 0) AS cnt_t2
        FROM zipf_trace
        LEFT JOIN (
            SELECT b, COUNT(*) AS cnt_t2
            FROM zipf_pingpong
            GROUP BY b
        ) AS t2_count
        ON zipf_trace.a = t2_count.b
        GROUP BY zipf_trace.a, cnt_t2
        ORDER BY cnt_t1 DESC
    """).fetchdf()
    plot_pingpong(df)
    
    unique_count = con.execute("SELECT COUNT(DISTINCT a) FROM zipf_trace").fetchone()[0]
    print("第二列去重后的数量:", unique_count)
    # 提取数据
    
    # x = result_trace['group_id']
    # y = result_trace['avg_freq']
    # print(len(x))
    # # 画图
    # plt.figure(figsize=(8, 5))
    # plt.plot(x, y, marker='o', linewidth=1.5, markersize=3)
    # plt.tight_layout()
    # plt.savefig('figure_38_5.png')


if __name__ == "__main__":
    log_path_1 = '/mnt/nvme/experiment_space/2025-10-13-16:47:12/server/graphscope_logs'
    # pingpong_log_1 = '/data-1/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2025-10-14-15:15:03/latency/thread_log_0.log' # 2GB
    # pingpong_log_2 = '/data-1/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2025-10-14-15:20:57/latency/thread_log_0.log' # 1GB
    # pingpong_log_3 = '/data-1/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2025-10-14-16:03:29/latency/thread_log_0.log' # 0.5GB
    # pingpong_log_4 = '/data-1/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2025-10-14-16:20:46/latency/thread_log_0.log' # 0.25GB
    
    # pingpong_log_5 = '/data-1/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2025-10-14-16:52:37/latency/thread_log_0.log'
    # traverse_thread_logs(log_path_1)
    pingpong_log_1 = "2025-10-16-09:40:54" # Sieve 1s 1GB
    pingpong_log_2 = "2025-10-16-09:58:40" # Clock 5s 1GB
    pingpong_log_3 = "2025-10-16-10:09:36" # Clock 5s 0.5GB
    pingpong_log_4 = "2025-10-16-10:23:18" # Sieve 1s 0.5GB
    pingpong_log_5 = '2025-10-20-20:19:20' # dSIEVE 1s 1GB
    pingpong_log_6 = '2025-10-21-14:13:29' # SIEVE
    pingpong_log_7 = '2025-10-21-14:16:53' # LRU
    pingpong_log_8 = '2025-10-21-14:22:52' # dSieve
    pingpong_log_9 = '2025-10-22-22:04:13' # Sieve 10s 1GB
    pingpong_log_10 ='2025-10-22-22:00:59' # Clock 10s 1GB
    pingpong_log_11 = '2025-10-22-22:19:52'

    log_id = 11
    pingpong_log = f'/data-1/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/{pingpong_log_11}/latency/thread_log_0.log' 
    analysis_pingpong(pingpong_log, log_id)
    
    data_path = "/data-2/zhengyang/traces/zipf/zipf_0.60.txt"
    pingpong_log_1 = "2025-10-16-10:49:38"# Sieve 10s 0.5GB
    pingpong_log_2 = "2025-10-16-10:58:39" # Sieve 10s 1GB
    pingpong_log_3 = '2025-10-16-14:08:10' # Sieve 10s 1GB
    pingpong_log = f'/data-1/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/{pingpong_log_1}/latency/thread_log_0.log'
    # analysis_zipf(data_path, pingpong_log)

