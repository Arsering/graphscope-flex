import os
import glob
import re
import numpy as np
import duckdb
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib as mpl
from matplotlib.ticker import FormatStrFormatter  # 导入格式化工具
from matplotlib.ticker import FixedLocator



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
    # thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 25]
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


    plot_pingpong(df, log_id)

def plot_pingpong(df, log_id):
    set_mpl()
    colors = ['#0070C0','#8E69B8','#C53A32','#529E3F','#EF8636']

    group_size = df.shape[0]/50
    df = (
        df.groupby(df.index // group_size)[["cnt_t1", "cnt_t2"]]
        .mean()
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
    ax2.plot(x, np.array(df["cnt_t2"])/np.array(df["cnt_t1"]), color=colors[1], marker="s")
    # ax2.plot(x, np.array(df["cnt_t2"]), color=colors[1], marker="s")

    # 轴标签
    ax1.set_xlabel("File Page Range (%, Sorted By Access Count)")
    ax1.set_ylabel("Avg. Access Count", color=colors[0])
    ax2.set_ylabel("Avg. Ping-Pong Count", color=colors[1])

    # 刻度颜色
    ax1.tick_params(axis='y', colors=colors[0])
    ax1.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    # ax1.set_ylim(-2e7, 6.2e8)
    ax1.set_yscale('log')  # 设置y轴为对数刻度（关键代码）
    ax1.set_ylim(1e-1, 1e5)
    ax1.set_xlim(0,100)
    ax1.yaxis.set_major_locator(FixedLocator([1, 1e2, 1e4]))



    ax2.tick_params(axis='y', colors=colors[1])
    # ax2.ticklabel_format(style='sci', axis='y', scilimits=(0,0))
    # ax2.set_ylim(-7.5, 37.5)
    # ax2.yaxis.set_major_locator(FixedLocator([0, 15, 30]))

    
    print(len(x), len(ax1.get_ylim()))
    max_y1 = max(ax1.get_ylim())
    ax1.fill_between(x, 0, max_y1, where=(x >= 0) & (x <= x[3]), 
                    color='#FF6B6B', alpha=0.3,edgecolor='none',
                 linewidth=0)
    # 10-80 区间：填充浅灰色
    ax1.fill_between(x, 0, max_y1, where=(x >= x[3]) & (x <= x[39]), 
                    color='#FFD166', alpha=0.3,edgecolor='none',
                 linewidth=0)
    # 80-100 区间：填充浅紫色
    ax1.fill_between(x, 0, max_y1, where=(x >= x[39]) & (x <= x[-1]), 
                    color='#6BAED6', alpha=0.3,edgecolor='none',
                 linewidth=0)

    # 图例（需要合并两个轴的 legend）
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    # ax1.legend(lines + lines2, labels + labels2, loc="upper right", frameon=False)

    # 网格与标题
    ax1.grid(True, linestyle="--", alpha=0.3)
    # plt.title("cnt_t1 (left) vs cnt_t2 (right)")

    plt.tight_layout()
    plt.savefig(f'figs/39_{log_id}.pdf')


if __name__ == "__main__":
    trace_log_path = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-16:54:19/server/graphscope_logs'
    sieve_eviction_log_path = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-17:51:47/server/graphscope_logs'
    dsieve_eviction_log_path = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-17:58:20/server/graphscope_logs'
    clock_eviction_log_path = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-21-19:23:06/server/graphscope_logs'
    
    dsieve_pingpong = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-22-23:33:02/server/graphscope_logs'
    sieve_pingpong = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-23-00:10:48/server/graphscope_logs'

    # sieve, time 1s
    pingpong_log_1 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-24-16:25:43/server/graphscope_logs'
    # sieve time 3s
    pingpong_log_2 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-24-16:32:11/server/graphscope_logs'
    # sieve time 10s
    pingpong_log_3 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-10-24-16:40:18/server/graphscope_logs'
    analysis_pingpong(trace_log_path, pingpong_log_2, 2)  

