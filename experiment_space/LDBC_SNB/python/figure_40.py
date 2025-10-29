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
    
def analysis_pingpong(trace_log_path, eviction_log_path, log_id):
    con = duckdb.connect("mydata.db")
    
    # print("Load trace log")
    # con.execute("DROP TABLE IF EXISTS trace_log")
    # con.execute("""
    #     CREATE TABLE trace_log (
    #         a UBIGINT
    #     )
    # """)
    # thread_logs = glob.glob(os.path.join(trace_log_path, "*thread_log*"))
    # thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 25]
    # for log_file in thread_logs:
    #     print(log_file)
    #     con.execute(f"""
    #         INSERT INTO trace_log
    #         SELECT 
    #             TRY_CAST(column0 AS UBIGINT) AS a,
    #         FROM read_csv_auto('{log_file}', delim=' ', header=false, ignore_errors=True, types={{
    #             'column0':'UBIGINT',
    #         }})
    #         WHERE TRY_CAST(column0 AS UBIGINT) IS NOT NULL
    #     """)

        
        
    print("Load eviction log")
    con.execute("DROP TABLE IF EXISTS eviction_log")
    con.execute("""
        CREATE TABLE eviction_log (
            a UBIGINT
        )
    """)
    thread_logs = glob.glob(os.path.join(eviction_log_path, "*thread_log*"))
    thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 0]
    for log_file in thread_logs:
        print(log_file)
        con.execute(f"""
            INSERT INTO eviction_log
            SELECT 
                TRY_CAST(column0 AS UBIGINT) AS a,
            FROM read_csv_auto('{log_file}', delim=' ', header=false, ignore_errors=True, types={{
                'column0':'UBIGINT',
            }})
            WHERE TRY_CAST(column0 AS UBIGINT) IS NOT NULL
        """)

        
        
    print("Count trace log")
    df = con.execute("""
        SELECT
            trace_log.a AS value,
            COUNT(*) AS cnt_t1,
            COALESCE(t2_count.cnt_t2, 0) AS cnt_t2
        FROM trace_log
        LEFT JOIN (
            SELECT a, COUNT(*) AS cnt_t2
            FROM eviction_log
            GROUP BY a
        ) AS t2_count
        ON trace_log.a = t2_count.a
        GROUP BY trace_log.a, cnt_t2   
        ORDER BY cnt_t1 DESC
    """).fetchdf()
    # print(df)
    
    filtered_df = df[df['cnt_t2'] > 1].copy()
    filtered_df['original_row_index'] = filtered_df.index
    print(filtered_df)
    # plot_pingpong(df)


    plot_pingpong_only(df, log_id)
    return df

def plot_pingpong(df, log_id):
    set_mpl()
    colors = ['#0070C0','#8E69B8','#C53A32','#529E3F','#EF8636']

    group_size = 36000
    df = (
        df.groupby(df.index // group_size)[["cnt_t1", "cnt_t2"]]
        .sum()
        .reset_index(drop=True)
    )
    n = len(df)
    print(df.head(max(1, int(n * 0.05)))['cnt_t1'].sum()/df['cnt_t1'].sum())
    
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
    # ax2.set_ylim(0, 0.13)

    # 图例（需要合并两个轴的 legend）
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    # ax1.legend(lines + lines2, labels + labels2, loc="upper right", frameon=False)

    # 网格与标题
    ax1.grid(True, linestyle="--", alpha=0.3)
    # plt.title("cnt_t1 (left) vs cnt_t2 (right)")

    plt.tight_layout()
    plt.savefig(f'figs/40_{log_id}.pdf')

def plot_pingpong_only(df, log_id):
    set_mpl()
    colors = ['#0070C0','#8E69B8','#C53A32','#529E3F','#EF8636']

    group_size = df.shape[0]/100
    df = (
        df.groupby(df.index // group_size)[["cnt_t1", "cnt_t2"]]
        .sum()
        .reset_index(drop=True)
    )
    n = len(df)
    print(df.head(max(1, int(n * 0.05)))['cnt_t1'].sum()/df['cnt_t1'].sum())
    print(df.tail(max(1, int(n * 0.4)))['cnt_t1'].sum()/df['cnt_t1'].sum())
    # print(df['cnt_t1'])
    # 创建图表和双 y 轴
    fig, ax1 = plt.subplots()

    # 横坐标为行号
    x = df.index
    x = np.array(x) /max(x)*100
    
    # 左 y 轴画 cnt_t1
    # ax2.plot(x, np.array(df["cnt_t2"])/np.array(df["cnt_t1"]), color="tab:red", marker="s", label="K")
    ax1.plot(x, np.array(df["cnt_t2"]), color=colors[1], marker="s")

    # 轴标签
    ax1.set_xlabel("File Page Range (%, Sorted By Access Count)")
    ax1.set_ylabel("Normalized Lifetime")

    # 刻度颜色
    ax1.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    
    # 网格与标题
    ax1.grid(True, linestyle="--", alpha=0.3)
    # plt.title("cnt_t1 (left) vs cnt_t2 (right)")

    plt.tight_layout()
    plt.savefig(f'figs/40_{log_id}.pdf')

def plot_lifetime_diff(df_sieve, df_dsieve, log_id):
    set_mpl()
    colors = ['#0070C0','#8E69B8','#C53A32','#529E3F','#EF8636']
    for id in range(len(df_sieve)):
        if df_sieve.loc[id, 'cnt_t1'] != df_dsieve.loc[id, 'cnt_t1']:
            print("fuck")
            
    group_size = 36000
    df_sieve = (
        df_sieve.groupby(df_sieve.index // group_size)[["cnt_t1", "cnt_t2"]]
        .sum()
        .reset_index(drop=True)
    )
    total_t2 = df_sieve["cnt_t2"].sum()
    df_sieve["cnt_t2"] = df_sieve["cnt_t2"] * 1000 / total_t2
    
    df_dsieve = (
        df_dsieve.groupby(df_dsieve.index // group_size)[["cnt_t1", "cnt_t2"]]
        .sum()
        .reset_index(drop=True)
    )
    total_t2 = df_dsieve["cnt_t2"].sum()
    df_dsieve["cnt_t2"] = df_dsieve["cnt_t2"] * 1000 / total_t2

    # 横坐标为行号
    x = df_sieve.index
    x = np.array(x) /max(x)*100
    y = np.array(df_dsieve["cnt_t2"])-np.array(df_sieve["cnt_t2"])
    y = y / max(abs(y))  # 归一化到 -1 到 1 之间
    
    # 2. 绘制柱状图
    plt.figure()
    plt.bar(x, y,color=[colors[2] if v >= 0 else colors[0] for v in y],width=1.0)  # height为y值，正负决定方向
    print(y)
    
    plt.axhline(0, color='black', linewidth=1)  # ✅ 在 y=0 画一条水平线
    plt.xlabel('File Page Range (%, Sorted By Access Count)')
    plt.ylabel('Lifetime Variance')
    plt.ylim(-1.1, 1.1)
    plt.yticks(np.arange(-1.0, 1.1, 0.5))


    # 4. 显示网格线（可选，更易读）
    plt.grid(axis='y', linestyle='--', alpha=0.3)


    plt.tight_layout()
    plt.savefig(f'figs/life_time_diff_{log_id}.pdf')

def hot_warm_cold_analysis():
    # 准备 DuckDB 环境和示例数据
    con = duckdb.connect("mydata.db")

    start_row = 150000000
    target_unique = 1310720

    query = """
        SELECT row_number() OVER () AS rn, a
        FROM trace_log
    """

    cursor = con.execute(query)

    # 用 dict 统计频次
    freq = defaultdict(int)
    seen = set()
    end_row = None

    while True:
        row = cursor.fetchone()
        if row is None:
            break

        rn, a = row
        if rn < start_row:
            continue

        freq[a] += 1
        seen.add(a)

        if len(seen) == target_unique:
            end_row = rn
            break

    # 统计只出现一次的值比例
    once_count = sum(1 for v in freq.values() if v == 1)
    total_unique = len(freq)
    ratio = once_count / total_unique if total_unique > 0 else 0

    print(f"start={start_row}, end={end_row}, unique={target_unique}")
    print(f"只出现一次的值：{once_count}/{total_unique} = {ratio:.2%}")
    # print("频率统计：", dict(freq))
    
if __name__ == "__main__":
    trace_log_path = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-16:54:19/server/graphscope_logs'
    
    # Sieve Eviction, sieve cache snapshot
    log_path_1 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-21:56:11/server/graphscope_logs'
    # dsieve eviction, pinned layer snapshot,256*4
    log_path_2 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-22:07:38/server/graphscope_logs'
    # dsieve eviction, sieve layer snapshot,256*4
    log_path_3 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-23:39:52/server/graphscope_logs'
    # dsieve eviction, pinned layer snapshot,256*1
    log_path_4 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-10:54:37/server/graphscope_logs'
    # dsieve eviction, pinned layer snapshot,256*16
    log_path_5 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-11:29:24/server/graphscope_logs'
    log_path_6 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-11:29:24/server/graphscope_logs'
    log_path_7 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-14:27:14/server/graphscope_logs'
    log_path_8 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-14:46:54/server/graphscope_logs'
    log_path_9 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-15:03:47/server/graphscope_logs'
    log_path_10 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-16:29:18/server/graphscope_logs'
    df_1 = analysis_pingpong(trace_log_path, log_path_1, 1)
    # df_2 = analysis_pingpong(trace_log_path, log_path_2, 2)
    # df_3 = analysis_pingpong(trace_log_path, log_path_3, 3)
    # df_4 = analysis_pingpong(trace_log_path, log_path_4, 4)
    # df_5 = analysis_pingpong(trace_log_path, log_path_5, 5)
    # df_6 = analysis_pingpong(trace_log_path, log_path_6, 6)
    # df_7 = analysis_pingpong(trace_log_path, log_path_7, 7)
    # df_8 = analysis_pingpong(trace_log_path, log_path_8, 8)
    # df_9 = analysis_pingpong(trace_log_path, log_path_9, 9)
    # df_10 = analysis_pingpong(trace_log_path, log_path_10, 10)
    # plot_lifetime_diff(df_10, df_1,1)
    # plot_lifetime_diff(df_1, df_3,0)
    # hot_warm_cold_analysis()