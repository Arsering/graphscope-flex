
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 16:44:14 2023
@author: yz
"""

import os
import sys
import re
import numpy as np
from matplotlib import pyplot as plt
import matplotlib as mpl
import matplotlib.font_manager as fm
from matplotlib.ticker import MultipleLocator, NullFormatter

import multiprocessing
import math

CUR_FILE_PATH = os.path.dirname(__file__)
sys.path.append(CUR_FILE_PATH + '/../')
# from plt_mine import set_mpl

def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            'font.family': 'Arial',
            # 'mathtext.fontset': 'stix',
            'font.size': 20,
            'figure.figsize': (10.0, 6 * 0.618),
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

def plot_figure_1(datas):
    set_mpl()
    fig = plt.figure()
    ax1 = fig.subplots()
    labels = ['TriCache (10%)','BufferPool (10%)','TriCache (50%)','BufferPool (50%)']
    plt.grid(True, linestyle='--', color='#f0f0f0', linewidth=1, zorder=0)

    for fig_id in [0]:
        ax1.plot(datas[fig_id][0], datas[fig_id][1], color=plt.get_cmap('tab10')(0), linestyle='-', zorder=10)
    print(min(datas[0][0]))
    x = np.array(datas[0][0])
    y = np.array(datas[0][1])
    plt.fill_between(x, y, where=(x >= min(x)) & (x <= 260), color='#99000d', edgecolor='none', alpha=0.5, label='Hot', zorder=10)
    plt.fill_between(x, y, where=(x > 260) & (x <= 14000), color='#74c476', edgecolor='none', alpha=0.5, label='Warm', zorder=10 )
    plt.fill_between(x, y, where=(x > 14000) & (x <= max(x)), color='#d9d9d9', edgecolor='none', alpha=0.5, label='Cold', zorder=10)
    plt.legend(loc='upper right', frameon=False)

    # 设置y轴为以2为底的对数坐标
    # plt.yscale('log', base=2)
    # # 自定义刻度标签为2ⁿ格式
    # plt.gca().yaxis.set_major_formatter(
    #     plt.FuncFormatter(lambda val, pos: f"$2^{{{int(np.log2(val))}}}$")
    # )
    ax1.set_xlim(0, 16000)
    # 在特定位置显示刻度
    plt.xticks(np.arange(4000, 16000, 4000),np.arange(4000, 16000, 4000))  # 选择需要显示的刻度
    
    
    ax1.set_ylim(0, 350)
    plt.yticks(range(0, 350, 50), range(0, 350, 50))
    
    plt.xlabel("Person ID")
    plt.ylabel('Number of Visits')

    plt.savefig('figs/fig32-1.pdf', bbox_inches='tight')

if __name__ == "__main__":
    # 指定要遍历的目录
    directory = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-04-17-19:20:46/server/graphscope_logs'

    # 正则表达式匹配文件名
    pattern = re.compile(r'thread_log_([1-9][0-9]*)\.log')

    # 遍历目录下的所有文件
    data = []
    for filename in os.listdir(directory):
        if pattern.match(filename):
            file_path = os.path.join(directory, filename)
            print(f'Found log file: {file_path}')
            # 在这里可以添加对每个 log 文件的处理逻辑
            f = open(file_path , 'r')
            line = f.readline()
            
            while line:
                logs = line.split(" ")
                data.append(int(logs[0]))
                line = f.readline()
            break
    
    values, counts = np.unique(data[0:10000000], return_counts=True)
    sorted_with_index = sorted(enumerate(counts), key=lambda x: -x[1])  # 加负号表示降序
    sorted_values = [value for index, value in sorted_with_index]
    
    datas = []
    range_len = 10
    data = 0
    for i in range(len(sorted_values)):
        data += sorted_values[i]
        if (i+1) % range_len == 0:
            datas.append(data/range_len)
            data = 0
    print(len(datas))
    print(min(sorted_values))
    plot_figure_1([[range(0, len(datas)), datas]])
    print('work finished\n')
