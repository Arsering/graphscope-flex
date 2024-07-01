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
        
def read_log(file_name, data_len):
    f = open(file_name , 'r')
    line = f.readline()
    line = f.readline()
    query_ids =[]
    datas = np.zeros((data_len, 30000)) 
    
    count = 0
    while line:
        if len(line.split(']')) < 2:
            break
        logs = line.split(']')[1].split('[')
        if logs[0].strip() == 'profiling:':
            query_ids.append(int(logs[1]))
            latencys = line.split(']')[2].split('[')[1].split('|')
            datas[0][count] = int(latencys[0])
            datas[1][count] = int(latencys[1])
            datas[2][count] = int(latencys[2])
            datas[3][count] = int(latencys[3])
            datas[4][count] = int(latencys[4])
            count += 1
        line = f.readline()
    print(len(query_ids))
    return query_ids, datas

if __name__ == "__main__":
    # MMAP
    file_path_1 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-16-20:52:14/server/gs_log.log'
    # Buffer Pool overall
    file_path_2 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-16-20:41:28/server/gs_log.log'
    # Buffer Pool get and Decode
    file_path_3 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-16-16:32:56/server/gs_log.log'

    query_ids_1, datas_1 = read_log(file_path_1, 5)
    query_ids_2, datas_2 = read_log(file_path_2, 5)
    query_ids_3, datas_3 = read_log(file_path_3, 5)
    # assert (np.array(query_ids_1) == np.array(query_ids_2)).all()
    
    data_range = range(int(len(query_ids_2)/2), len(query_ids_2))
    # per = (np.array(latency_1_0[int(len(latency_1_0)/2):]) - np.array(latency_2_0[int(len(latency_1_0)/2):])) / np.array(latency_1_0[int(len(latency_1_0)/2):])
    avg = np.array(datas_3[3][data_range])/np.array(datas_3[4][data_range])
    # print(len(avg))

    print("P99:", np.percentile(avg/2.5, 50))
    
    print('work finished\n')


