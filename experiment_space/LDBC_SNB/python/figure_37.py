import matplotlib.pyplot as plt
import numpy as np
import re
import os
import matplotlib as mpl


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
            'lines.linewidth': 3,
            'lines.markersize': 5,
            # 'axes.labelweight': 'bold'
        }
    )
    
def plot_fig():
    set_mpl()
    # 示例数据
    categories = [f'{i}%' for i in [30, 60, 90]]  # X轴标签

    value_1 = [0.089815, 0.157106, 0.603523]
    value_2 = [0.0759544, 0.131001, 0.483646]
    
    value_1 = [0.849742, 1.474756, 5.718934]
    value_2 = [0.650852, 1.12469, 4.13901]

    x = np.arange(len(categories))  # X轴位置
    width = 0.35                    # 柱子的宽度

    fig, ax = plt.subplots(figsize=(10, 6))

    # 画柱状图
    rects1 = ax.bar(x - width/2, value_1, width, label='GoCache')
    rects2 = ax.bar(x + width/2, value_2, width, label='VMCache')



    # 添加标签和标题
    ax.set_ylabel('Throughput (GB)')
    ax.set_xlabel('Memory / Data Size')

    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=45)
    ax.legend(loc='upper left')

    # # 添加数值标注
    # def add_labels(rects):
    #     for rect in rects:
    #         height = rect.get_height()
    #         ax.annotate(f'{height}',
    #                     xy=(rect.get_x() + rect.get_width() / 2, height),
    #                     xytext=(0, 3),  # 偏移
    #                     textcoords="offset points",
    #                     ha='center', va='bottom')

    # add_labels(rects1)
    # add_labels(rects2)

    plt.tight_layout()
    plt.savefig('figs/fig37-1.pdf', bbox_inches='tight')
    plt.savefig('figs/fig37-1.svg', bbox_inches='tight')


# 示例使用
if __name__ == "__main__":
    plot_fig()