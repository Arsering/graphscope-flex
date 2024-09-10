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
        
def read_log(file_name):
    f = open(file_name , 'r')
    line = f.readline()
    line = f.readline()
    query_ids =[]
    latency_0 =[]
    latency_1 = []
    
    count = 0
    while line:
        if len(line.split(']')) < 2:
            break
        logs = line.split(']')[1].split('[')
        if logs[0].strip() == 'profiling:':
            query_ids.append(int(logs[1]))
            latencys = line.split(']')[2].split('[')[1].split('|')
            latency_0.append(int(latencys[0]))
            latency_1.append(int(latencys[1]))
        # if count == 43:
        #     break
        # count += 1
        line = f.readline()
    print(len(query_ids))
    return query_ids, latency_0, latency_1

def get_fd_filename(dir_path):
    f = open(dir_path , 'r')
    filenames = []
    line = f.readline()
    count = 0
    
    while line:
        logs = line.split(" ")
        if len(logs) > 3 and "io_backend.h:30]" in logs[3]:
            logs = line.split("|")
            filenames.append((logs[1], int(logs[2]), int(logs[3]), int(logs[4])))
            count += 1
        line = f.readline()
    return filenames

def file_name_classification(results, word_suffixs):
    new_results = np.zeros((len(word_suffixs), 3), dtype=int) 

    for j in range(0, len(results)):
        for i in range(0, len(word_suffixs)):
            if word_suffixs[i] in results[j][0] and int(results[j][2])+int(results[j][3]) !=0:
                new_results[i][0] += int(results[j][1])
                new_results[i][1] += int(results[j][2])
                new_results[i][2] += int(results[j][3])
    return new_results

def plot_figure(datas, labels):
    set_mpl()
    fig = plt.figure()
    ax1 = fig.subplots()
    point_num = 1000

    print((datas[0]))
    print((datas[1]))
    data_num = max(len(datas[0]), len(datas[1]))
    plt.xlim(0, data_num+1)
    plt.xticks(range(0, data_num, 10), range(0, data_num, 10))
    plt.xlabel('File ID')
    
    ax1.plot(range(0, len(datas[0]), 1), datas[0]*100, color=plt.get_cmap('tab10')(0))
    ax1.set_ylabel('File Access CDF (%)', color=plt.get_cmap('tab10')(0))
    ax1.set_ylim(0, 100)
    ax1.set_yticks(range(0, 101, 20), range(0, 101, 20))
    
    ax2 = ax1.twinx()
    ax2.plot(range(0, len(datas[1])), datas[1], color=plt.get_cmap('tab10')(2))
    ax2.set_ylabel('Accumulate File Size (GB)', color=plt.get_cmap('tab10')(2))
    ax2.set_ylim(0, 17)
    ax2.set_yticks(range(0, 16, 5), range(0, 16, 5))


    # plt.grid(True, linestyle='--', color='gray', linewidth=0.5)
    
    plt.savefig('figs/fig4.pdf', bbox_inches='tight')



if __name__ == "__main__":
    word_suffixs = ['.keys', '.indices', '.nbr', '.adj', '.col_', '.items', '.data']

    # sieve 50%
    gs_log_path_1 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-03-20:13:45/server/gs_log.log'
    gs_log_path_2 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-03-21:31:22/server/gs_log.log'
    gs_log_path_3 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-08-13-15:32:12/server/gs_log.log'
    gs_log_path_4 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-09-09-14:11:54/server/gs_log.log'
    results = get_fd_filename(gs_log_path_1)
    new_results = file_name_classification(results, word_suffixs)
    for i in range(0, new_results.shape[0]):
        # print(new_results[i])
        print(word_suffixs[i], end='\t')
        print(round(new_results[i][2]*1000/(new_results[i][1]+new_results[i][2]),2), end='\t')
        print(round(new_results[i][0]/1024/1024/1024,2), end='\t')
        print(round((new_results[i][1]+new_results[i][2])/1000000,0), end='\t')
        print(round((new_results[i][2])/1000000,2))

    ret = [0, 0]
    for i in [0, 1, 2, 3, 4, 5, 6]:
        ret[0] += new_results[i][2]
        ret[1] += new_results[i][1] + new_results[i][2]
    
    for i in [0, 1, 2, 3, 4, 5, 6]:
        print((new_results[i][1] + new_results[i][2])/ret[1])

    print(round(ret[0]*1000/ret[1]))

    hot_ratio = []
    new_results = []

    for item in results:
        if item[2] + item[3] > 1:
            hot_ratio.append((item[3]+item[2]))
            new_results.append(item)
    datas = []
    out = np.argsort(-np.array(hot_ratio), axis=0)
    datas.append(np.cumsum(np.take_along_axis(np.array(hot_ratio), out, axis=0)/np.sum(hot_ratio)))    
    
    new_results_t = np.take_along_axis(np.array([row[1] for row in new_results]), out, axis=0)
    datas.append(np.cumsum(new_results_t)/1024/1024/1024)
    
    new_results_t = np.take_along_axis(np.array([row[0] for row in new_results]), out, axis=0)
    print(np.take_along_axis(np.array(hot_ratio), out, axis=0)/np.sum(hot_ratio))
    print(new_results_t)
    # new_results_t = np.take_along_axis(np.array([row[2]+row[3] for row in new_results]), out, axis=0)
    # print(np.cumsum(new_results_t)[1]/np.sum(new_results_t))

    # plot_figure(datas, [])

    count = 0
    # for id in out:
    #     count += 1
    #     # if count == 10:
    #     #     break
    #     print(id, end='\t')
    #     print("{:.15f}".format((new_results[id][2]+new_results[id][3])/np.sum(new_results_t)), end='\t')
    #     print(new_results[id][3]/(new_results[id][2]+new_results[id][3]), end='\t')

    #     print("{:.5f}".format(new_results[id][1]/1024/1024), end='\t')
    #     print(new_results[id][0])



    print('work finished\n')


