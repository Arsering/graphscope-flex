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
            'font.sans-serif': 'Times New Roman',
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
            filenames.append((logs[1], logs[2], logs[3], logs[4]))
            count += 1
            
        line = f.readline()
    print(count)
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


# def read_trace(file_name):
#     f = open(file_name , 'r')
#     line = f.readline()

#     while line:


if __name__ == "__main__":
    word_suffixs = ['.keys', '.indices', '.nbr', '.adj', '.col_', '.items', '.data']

    # sieve 50%
    gs_log_path_1 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-03-20:13:45/server/gs_log.log'
    gs_log_path_2 = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-07-03-21:31:22/server/gs_log.log'
    results = get_fd_filename(gs_log_path_1)
    new_results = file_name_classification(results, word_suffixs)
    for i in range(0, new_results.shape[0]):
        # print(new_results[i])
        print(word_suffixs[i], end='\t')
        print(round(new_results[i][2]*1000/(new_results[i][1]+new_results[i][2]),2), end='\t')
        print(round(new_results[i][0]/1024/1024/1024,2), end='\t')
        print(round((new_results[i][1]+new_results[i][2])/1000000,0), end='\t')
        print(round((new_results[i][2])/1000000,2))
        
    cache_hit_ratios = []
    for item in results:
        if int(item[2])+int(item[3]) == 0:
            cache_hit_ratios.append(10000000)
            continue

        cache_hit_ratios.append(int(item[3])*1000/(int(item[3])+int(item[2])))
    out = np.argsort(np.array(cache_hit_ratios))

    count1 = [0, 0]
    count2 = [0, 0]
    for id in out:
        if cache_hit_ratios[id] != 10000000:
            print(results[id][0])
            print(int(results[id][1])/1024/1024, end='\t')
            print(int(results[id][2]), end='\t')
            print(int(results[id][3]), end='\t')
            print(cache_hit_ratios[id])
            count1[0] += int(results[id][2]) + int(results[id][3])
            count2[0] += int(results[id][1])

            if cache_hit_ratios[id] < 100 :
                count1[1] += int(results[id][2]) + int(results[id][3])
                count2[1] += int(results[id][1])
    print(count1[0])
    print(count1[1]/count1[0])
    print(count2[1])
    print('work finished\n')


