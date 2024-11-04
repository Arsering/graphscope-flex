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

    for fig_id in [0, 1]:
        ax1.plot(range(datas.shape[0]), datas[:, fig_id], color=plt.get_cmap('tab10')(fig_id), label=labels[fig_id])
    plt.legend(loc='upper right')
    plt.grid(True, linestyle='--', color='gray', linewidth=0.5)

    
    ax1.set_xlim(-5, 105)
    plt.xticks(range(0, 105, 20), range(0, 105, 20))
    ax1.set_ylim(0, 120000)
    plt.yticks(range(0, 120000, 20000), range(0, 120, 20))
    
    plt.xlabel('Time Line')
    plt.ylabel('KQPS')
    
    plt.savefig('figs/fig23-3.pdf', bbox_inches='tight')

def data_analysis_inner(file_path, datas, count):
    f = open(file_path , 'r')
    line = f.readline()
    
    while line:
        logs = line.split(" ")
        datas[count, 0] = int(logs[0])
        datas[count, 1] = int(logs[1])
        datas[count, 2] = int(logs[2])
        count += 1
        line = f.readline()
    return count

def data_analysis(dir_path):
    datas = np.zeros([3000000, 3])

    count = 0
    for root, dirs, files in os.walk(dir_path):
            for file in files:
                if "thread_log" not in file:
                    continue
                file_path = os.path.join(root, file)
                count = data_analysis_inner(file_path, datas, count)
    print(count)
    print(np.mean(datas[0:count, 0] - datas[0:count, 1])/2.7/1000)
    count_tmp = 0
    latency = 0
    datas_sum = np.zeros([30, 2])
    for id in range(0, count):
        datas_sum[int(datas[id, 2])][0] += datas[id, 0] - datas[id, 1]
        datas_sum[int(datas[id, 2])][1] += 1
    # return (datas_sum[1:22, 0]/2.7/datas_sum[1:22, 1]/1000)
    return datas, count

if __name__ == "__main__":
    data_dir_paths_3 = ['/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-15:32:18/server/graphscope_logs',
                        '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-14:19:00/server/graphscope_logs',
                        '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-17:24:26/server/graphscope_logs']
    
    datas, count = data_analysis(data_dir_paths_3[2])
    max_min_ts = [datas[0][1], datas[0][1]]
    range_num = 10
    for id in range(count):
        item = datas[id]
        if item[1] == 0:
            print('error')
        if item[1] > max_min_ts[0]:
            max_min_ts[0] = item[1]
        if item[1] < max_min_ts[1]:
            max_min_ts[1] = item[1]
    counts = np.zeros([range_num, 31])
    ranges = np.array(range(0, range_num+1)) * (max_min_ts[0]-max_min_ts[1])/range_num + max_min_ts[1]
    for item in datas:
        for idx in range(range_num):
            if item[1] >= ranges[idx] and item[1] < ranges[idx+1]:
                if int(item[2]) > 21:
                    counts[idx, 0] += 1
                else:
                    counts[idx, 1] += 1
    print(counts[:, 0:2])
    datas = counts[:, 0:2]
    labels = ['Write', 'Read']
    # plot_figure(datas, labels)
    
    print('\nwork finished')


