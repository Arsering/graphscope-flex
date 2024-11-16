import json
from tqdm import tqdm
import matplotlib.pyplot as plt
import numpy as np
import matplotlib as mpl

PREFIX = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/python/data/figure_25'

def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            # 'font.sans-serif': 'Times New Roman',
            'mathtext.fontset': 'stix',
            'font.size': 20,
            'figure.figsize': (20.0, 10 * 0.618),
            'savefig.dpi': 10000,
            'axes.labelsize': 25,
            'axes.linewidth': 1.2,
            'xtick.labelsize': 20,
            'ytick.labelsize': 20,
            # 'legend.loc': 'upper right',
            'lines.linewidth': 2,
            'lines.markersize': 5

        }
    )

def plot_figure_2(index, legends, xticks):
    breakdown = np.array([[265, 2507],
                          [268, 2497.8],
                          [282, 2587]])
    # 生成x轴的坐标
    bar_width=0.2
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(breakdown)
    print(index[0])
    colors = ['red', 'green', 'blue', 'brown']

    for idx in range(breakdown.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)

    ax1.legend(legends,loc='upper left')
    plt.xticks(range(1, breakdown.shape[0]), xticks, fontsize=20)
    # plt.ylim((0, 2800))
    # plt.yticks(np.array(range(0, 2600, 500)), np.array(range(0, 2600, 500)))
    

    plt.xlabel('Memory/Working_Set (%)')
    plt.ylabel('SSD IO (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig21-2.pdf', bbox_inches='tight')

 
def process_result_dict(dict_file, ax, fig_id, label_name):
    # 读取 JSON 文件
    with open(dict_file, 'r') as f:
        data = json.load(f)
    # 将数据转换为列表并按 'Proportion of Message Size' 排序
    sorted_data = sorted(data.items(), key=lambda x: int(x[0]), reverse=False)
    # 初始化类型、message和other页面的数量列表
    types = []
    message_pages = []
    other_pages = []
    propotion=[]

    # 计算每个type的页面数量并存储
    for key, values in sorted_data:
        types.append(key)
        index=len(types)
        message_pages.append(values["Avg Message Pages"])
        other_pages.append(values[" Avg Other Pages"])
        # if key=='7':
        #     print(values)
        #     print(values["Avg Message Size"] / 4096 + 1)
        #     print(values[" Avg Other Size"] / 4096)
        #     print(message_pages[index-1])
        #     print(other_pages[index-1])
    # 设置柱状图参数
    # print(types)
    # print(message_pages)
    # print(other_pages)
    x = np.arange(len(types))
    width = 0.4

    # 绘制 message 和 other 页面数量的堆叠柱状图
    bar1 = ax.bar(x+(fig_id-1)*0.2, np.array(message_pages)+np.array(other_pages), width, label=label_name, color=plt.get_cmap('tab10')(fig_id), zorder=100)
    # bar2 = ax.bar(x, other_pages, width, bottom=message_pages, label='Other Pages', color=plt.get_cmap('tab10')(2), zorder=100)

    for i, bar in enumerate(bar1):
        th = 0
        if bar.get_height() == 0:
            th=0.1
        ax.text(
            bar.get_x() + bar.get_width() / 2 + (fig_id-1)*0.08, 
            bar.get_height()+th,  # 提前一点点放置，使文字不会太贴近顶部
            f'{int(message_pages[i]+other_pages[i])}', 
            ha='center', va='bottom', color=plt.get_cmap('tab10')(fig_id), fontsize=12, fontweight='bold', zorder=101
        )

        # bar.set_edgecolor('grey')  # 设置边框颜色为黑色
        # bar.set_linewidth(0.5)        # 设置边框宽度为2

    # 设置对数坐标轴
    ax.set_yscale('log')
    ax.set_ylim(0.1, max(message_pages) + max(other_pages) + 200000)  # 将 y 轴最小值设置为 0.1

    # 添加标签和标题
    ax.set_xlabel('Complex Read ID')
    ax.set_ylabel('Number of Pages')
    # ax.set_title('Message and Other Pages by Type (Log Scale)')
    # ax.set_ylabel('Number of Pages')
    # ax.set_title('Message and Other Pages by Type')
    ax.set_xticks(x)
    ax.set_xticklabels(types)
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

if __name__ == "__main__":
    set_mpl()
    ig, ax = plt.subplots()
    fig_id = 0
    
    ratio = 100
    dict_file = PREFIX + f'/result_dict_{ratio}.json'
    label_name = f'Unorder_Ratio = {100-ratio}%'

    process_result_dict(dict_file, ax, fig_id, label_name)
    fig_id = 2

    ratio = 98
    dict_file = PREFIX + f'/result_dict_{ratio}.json'
    label_name = f'Unorder_Ratio = {100-ratio}%'

    process_result_dict(dict_file, ax, fig_id, label_name)
    
    ax.legend(loc='best')
    # 显示图形
    plt.savefig('figs/fig25.pdf', bbox_inches='tight')