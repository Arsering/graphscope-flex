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
            'lines.linewidth': 2,
            'lines.markersize': 5

        }
    )

def decode_file_1(file_name):
    f = open(file_name, 'r')
    results = np.zeros((2, 4000001), dtype=int)

    line = f.readline()
    length = 0
    while line:
        logs = line.split(' | ')
        results[0][length] = int(logs[1])
        results[1][length] = int(logs[2])
        length += 1
        line = f.readline()
    return length, results
    
def plot_figure(data):
    set_mpl()
    fig = plt.figure()
    ax1 = fig.subplots()
    point_num = 10000
    for query_id in [0]:
        hist, bin_edges = np.histogram(data, point_num)
        ax1.plot(np.array(bin_edges[0:point_num])/2.7/1000000, np.cumsum(hist[0:point_num])/len(data)*100, color=plt.get_cmap('tab10')(query_id))
        print(f"P20:\t{np.percentile(data, 20)/2.7}")
        print(f"P30:\t{np.percentile(data, 30)/2.7}")
        print(f"P50:\t{np.percentile(data, 50)/2.7}")
        print(f"P80:\t{np.percentile(data, 80)/2.7}")
        print(f"P90:\t{np.percentile(data, 90)/2.7}")
        print(f"P99:\t{np.percentile(data, 99)/2.7}")
        print(f"P99.9:\t{np.percentile(data, 99.9)/2.7}")


    ax1.set_xlim(0, 101)
    plt.xticks(range(0, 101, 30), range(0, 101, 30))
    ax1.set_ylim(0, 100)
    plt.xticks(range(0, 101, 20), range(0, 101, 20))
    
    plt.xlabel('Latency (ms)')
    plt.ylabel('CDF (%)')
    
    plt.savefig('fig6-1.pdf', bbox_inches='tight')

if __name__ == "__main__":
    # MMAP 50%
    file_name_1 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-04-16:08:33/server/graphscope_logs/thread_log_0.log'
    # BufferPool sieve 50%
    file_name_2 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-04-21:42:20/server/graphscope_logs/thread_log_0.log'
    # MMAP 20%
    file_name_3 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-05-23:20:12/server/graphscope_logs/thread_log_0.log'
    # BufferPool sieve 20%
    file_name_4 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-05-11:59:10/server/graphscope_logs/thread_log_0.log'
    length, results = decode_file_1(file_name_4)
    print(length)
    plot_figure(results[1][int(length/2):])

    print('work finished\n')


