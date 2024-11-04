import pickle
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib as mpl

def plot_from_pickle(file):
    with open(file,'rb') as json_file:
        # json.dump(sorted_degrees,json_file,indent=4)
        degree_dict=dict(pickle.load(json_file))
    # Filter degrees less than 500
    filtered_degrees = {k: v for k, v in degree_dict.items() if k < 500}

    # Separate keys and values for plotting
    degrees = list(filtered_degrees.keys())
    total_count = sum(filtered_degrees.values())
    proportions = [count / total_count for count in filtered_degrees.values()]

    # Plot the histogram with proportions
    plt.figure(figsize=(10, 6))
    plt.bar(degrees, proportions, color='skyblue')
    plt.xlabel('Degree')
    plt.ylabel('Proportion')
    plt.title('Proportion Histogram of Degree Frequencies (Degree < 500)')
    plt.tight_layout()
    plt.show()
    plt.savefig('/data-1/yichengzhang/data/data_processing/new_graph_results/30/res.svg')

def plot_from_pickle2(file, interval=100):
    with open(file, 'rb') as json_file:
        degree_dict = dict(pickle.load(json_file))
    
    # 筛选度数小于500的值
    filtered_degrees = {k: v for k, v in degree_dict.items() if k < 500}
    
    # 将度数按指定区间进行分组
    bins = range(0, 500, interval)
    binned_counts = {f"{i}-{i + interval - 1}": 0 for i in bins}

    for degree, count in filtered_degrees.items():
        bin_key = f"{(degree // interval) * interval}-{((degree // interval) * interval) + interval - 1}"
        if bin_key in binned_counts:
            binned_counts[bin_key] += count
    
    # 计算每个区间的比例
    total_count = sum(binned_counts.values())
    bin_labels = list(binned_counts.keys())
    proportions = [count / total_count for count in binned_counts.values()]

    # 绘制柱状图
    plt.figure(figsize=(10, 6))
    plt.bar(bin_labels, proportions, width=1.0, color='skyblue', align='center')
    plt.xlabel('Degree Range')
    plt.ylabel('Proportion')
    plt.title('Proportion Histogram of Degree Frequencies by Range (Degree < 500)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('/data-1/yichengzhang/data/data_processing/new_graph_results/30/res.svg')
    plt.show()

def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            # 'font.sans-serif': 'Times New Roman',
            'mathtext.fontset': 'stix',
            'font.size': 20,
            'figure.figsize': (10.0, 10 * 0.618),
            'savefig.dpi': 10000,
            'axes.labelsize': 25,
            'axes.linewidth': 1.2,
            'xtick.labelsize': 20,
            'ytick.labelsize': 20,
            'legend.loc': 'upper right',
            'lines.linewidth': 2,
            'lines.markersize': 5

        }
    )
    
def plot_from_pickle3(file, max_value=500):
    with open(file, 'rb') as json_file:
        degree_dict = dict(pickle.load(json_file))
    
    # 将数据加载到 DataFrame 并计算每个值的出现次数
    counts_df = pd.DataFrame(list(degree_dict.items()), columns=['Degree', 'Count'])

    # 计算所有计数的总和
    total_counts = counts_df['Count'].sum()
    
    # 计算加权平均值
    weighted_average_degree = (counts_df['Degree'] * counts_df['Count']).sum() / total_counts
    print(f'所有度数的加权平均值: {weighted_average_degree:.4f}')  # 打印加权平均值，保留4位小数

    # 计算小于 max_value 的比例总和
    filtered_counts_df = counts_df[counts_df['Degree'] < max_value]
    proportion_sum = filtered_counts_df['Count'].sum() / total_counts
    print(f'小于{max_value}的点的比例总和: {proportion_sum:.4f}')  # 打印比例总和，保留4位小数

    # 创建均分的 bins，确保包括 0 到 max_value 的范围
    custom_bins = list(range(0,120,30))+list(range(120,max_value+50,50))

    # 按自定义分区进行分组，并计算每个区间的频率
    counts_df['Range'] = pd.cut(counts_df['Degree'], bins=custom_bins)
    range_counts = counts_df.groupby('Range')['Count'].sum()
    
    # 计算比例时使用总计数
    range_proportion = range_counts / total_counts  # 计算比例

    # 提取区间的左端点和宽度，用于绘图
    left_edges = [interval.left for interval in range_proportion.index]
    widths = [interval.length for interval in range_proportion.index]

    set_mpl()
    # 绘制柱状图，横跨区间
    plt.figure(figsize=(12, 8))
    plt.bar(left_edges, range_proportion*100, width=widths, align='edge', color=plt.get_cmap('tab10')(0), zorder=100)
    # left_edges = left_edges[0:10:]

    plt.xlabel('Degree')
    plt.ylabel('Proportion (%)')
    # plt.title('Proportion Histogram of Degree Frequencies (Degree < {})'.format(max_value))
    plt.xticks(left_edges + [custom_bins[-1]], rotation=45, ha='right')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    plt.savefig('figs/fig24.pdf', bbox_inches='tight')

file='/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/python/data/in_result.pkl'
plot_from_pickle3(file)