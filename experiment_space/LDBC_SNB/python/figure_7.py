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

import multiprocessing
import math

CUR_FILE_PATH = os.path.dirname(__file__)
sys.path.append(CUR_FILE_PATH + '/../')
# from plt_mine import set_mpl

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
        
def read_data(file_path, data):
    f = open(file_path , 'r')
    line = f.readline()
    count = 0 
    
    while line:
        logs = line.split(" ")
        # data.append((int(logs[0]), int(logs[1]), int(logs[2])))
        data.append(int(logs[2]))
        count += 1
        line = f.readline()
    return count

def plot_figure(datas, labels):
    set_mpl()
    fig = plt.figure()
    ax1 = fig.subplots()
    point_num = 1000

    for fig_id in [0, 1, 2]:
        hist, bin_edges = np.histogram(datas[fig_id], point_num*1000)
        ax1.plot(np.array(bin_edges[0:point_num]), np.cumsum(hist[0:point_num])/len(datas[fig_id])*100, color=plt.get_cmap('tab10')(fig_id), label=labels[fig_id])
        print_percentile(datas[fig_id])
    plt.legend(loc='lower right')
    plt.grid(True, linestyle='--', color='gray', linewidth=0.5)

    
    ax1.set_xlim(0, 4096*2)
    plt.xticks(range(0, 4096*2, 1024), range(0, 8, 1))
    ax1.set_ylim(0, 100)
    plt.yticks(range(0, 101, 20), range(0, 101, 20))
    
    plt.xlabel('Visited Size (KB)')
    plt.ylabel('CDF (%)')
    
    plt.savefig('figs/fig7.pdf', bbox_inches='tight')

def print_percentile(data):
    print(f"P20:\t{np.percentile(data, 20)}")
    print(f"P30:\t{np.percentile(data, 30)}")
    print(f"P50:\t{np.percentile(data, 50)}")
    print(f"P80:\t{np.percentile(data, 80)}")
    print(f"P90:\t{np.percentile(data, 90)}")
    print(f"P99:\t{np.percentile(data, 99)}")
    print(f"P99.9:\t{np.percentile(data, 99.9)}")

    
if __name__ == "__main__":
    word_suffixs = ['.keys', '.indices', '.nbr', '.adj', '.col_', '.items', '.data']

    # bufferpool sf30 50%
    gs_log_paths = ['/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-08-16-14:03:03/server/graphscope_logs',
    # bufferpool sf100 50%
        '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-08-16-17:07:49/server/graphscope_logs',
    # bufferpool sf30 20%
        '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-08-16-19:39:29/server/graphscope_logs']
    
    datas = []
    for exp_id in [0, 1, 2]:
        data = []
        print(gs_log_paths[exp_id])
        for root, dirs, files in os.walk(gs_log_paths[exp_id]):
            for file in files:
                # 获取文件的完整路径
                if "thread_log" in file:
                    numbers = re.findall(r'\d+', file)
                    if int(numbers[0]) <= 29 or int(numbers[0]) == 60:
                        continue
                    print(file)
                    file_path = os.path.join(root, file)
                    count = read_data(file_path, data)
                    # break
        datas.append(data)
                
    # for file_id in range(1, 261, 1):
    #     data_t = []
    #     for item in data:
    #         if item[0] == file_id:
    #             data_t.append(item[2])
    #     if len(data_t) > 10 and np.percentile(data_t, 50) < 1024:
    #         print(f"{file_id}\t{len(data_t)}\t{np.percentile(data_t, 50)}")
            # print_percentile(data_t)
    # print(len(data))
    labels = ['sf30+50%', 'sf30+20%', 'sf100+50%']
    plot_figure(datas, labels)
    print('\nwork finished')


