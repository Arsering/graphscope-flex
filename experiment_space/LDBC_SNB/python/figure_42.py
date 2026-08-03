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
    thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 19]

    con = duckdb.connect("mydata.db")
    con.execute("DROP TABLE IF EXISTS mytable")
    con.execute("""
        CREATE TABLE mytable (
            a UBIGINT,
            b UBIGINT
        )
    """)
    
    for log_file in thread_logs:
        filename = os.path.basename(log_file)
        thread_num = get_thread_number(filename)
        print(f"\nProcessing: {filename} (Thread {thread_num})")
        print(log_file)
        
        con.execute(f"""
            INSERT INTO mytable
            SELECT 
                TRY_CAST(column0 AS UBIGINT) AS a,
                TRY_CAST(column1 AS UBIGINT) AS b
            FROM read_csv_auto('{log_file}', delim=' ', header=false, ignore_errors=True, types={{
                'column0':'UBIGINT',
                'column1':'UBIGINT'
            }})
            WHERE TRY_CAST(column0 AS UBIGINT) IS NOT NULL
              AND TRY_CAST(column1 AS UBIGINT) IS NOT NULL
        """)
        # break
    
    row_count = con.execute("SELECT COUNT(*) FROM mytable").fetchone()[0]
    print(f"mytable 的总行数：{row_count}")
    
    # 1. 从数据库获取每个a值的出现次数，并按次数降序排序
    # （假设con是数据库连接，返回结果为[(a1, count1), (a2, count2), ...]）
    a_occurrence = con.execute("""
        SELECT a, COUNT(*) AS occurrence_count
        FROM mytable 
        GROUP BY a
        ORDER BY occurrence_count DESC
    """).fetchall()

    # 2. 转换为pandas DataFrame
    df = pd.DataFrame(a_occurrence, columns=['a', 'occurrence_count'])

    # 3. 确定分组大小（分成100组）
    total = len(df)
    print("total ", total)
    num_groups = 50
    # 计算每组的基础大小（若总数据不能被100整除，最后几组会略小，用整数除法自动适配）
    group_size = (total + num_groups - 1) // num_groups  # 向上取整，确保分组数<=100

    # 4. 用索引整数除法分组，并计算每组的次数均值
    # 类似 df.groupby(df.index // group_size)[["occurrence_count"]].mean()
    grouped_df = (
        df.groupby(df.index // group_size)  # 按索引//group_size分组（0~group_size-1为第0组，以此类推）
        [["occurrence_count"]]  # 只关注次数列
        .sum()  # 计算每组均值
        .reset_index(drop=True)  # 重置索引（组号从0开始）
    )

    # 5. 为结果添加组号列（1~100）
    grouped_df['group_num'] = range(1, len(grouped_df) + 1)
    # 调整列顺序（组号在前，均值在后）
    grouped_df = grouped_df[['group_num', 'occurrence_count']].rename(columns={
        'occurrence_count': 'avg_occurrence'
    })

    # 打印结果
    print(grouped_df)
    return grouped_df

def plot_pingpong(df_1, df_2, log_id):
    set_mpl()
    colors = ['#0070C0','#8E69B8','#C53A32','#529E3F','#EF8636']
    
    # 创建图表和双 y 轴
    fig, ax1 = plt.subplots()

    # 横坐标为行号
    x = df_1['group_num'].to_numpy()
    x = np.array(x) /max(x)*100
    y_1 = df_1["avg_occurrence"].to_numpy()
    y_1 = y_1/sum(y_1)
    y_2 = df_2["avg_occurrence"].to_numpy()
    y_2 = y_2/sum(y_2)

    
    # 左 y 轴画 cnt_t1
    ax1.plot(x, y_1, color=colors[0], marker="o")
    ax1.plot(x, y_2, color=colors[1], marker="x")

    # 轴标签
    ax1.set_xlabel("File Page Range (%, Sorted By Access Count)")
    ax1.set_ylabel("Avg. Access Count", color=colors[0])

    # 刻度颜色
    ax1.tick_params(axis='y', colors=colors[0])
    ax1.set_yscale('log')  # 设置y轴为对数刻度（关键代码）

    # 图例（需要合并两个轴的 legend）
    lines, labels = ax1.get_legend_handles_labels()

    # 网格与标题
    ax1.grid(True, linestyle="--", alpha=0.3)
    # plt.title("cnt_t1 (left) vs cnt_t2 (right)")

    plt.tight_layout()
    plt.savefig(f'figs/42_{log_id}.pdf')
    

if __name__ == "__main__":
    log_path_1 = '/mnt/nvme0n1/zhengyang/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-11-09-22:43:21/server/graphscope_logs'
    log_path_2 = '/mnt/nvme0n1/zhengyang/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-11-10-01:52:28/server/graphscope_logs'
    df_1 = traverse_thread_logs(log_path_1)
    df_2 = traverse_thread_logs(log_path_2)
    plot_pingpong(df_1, df_2,1)
    

