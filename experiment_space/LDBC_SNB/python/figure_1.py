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

if __name__ == "__main__":
    # # MMAP
    # file_path_1 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-12-14:07:13/server/gs_log.log'
    # # Buffer Pool overall
    # file_path_2 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-14-13:10:54/server/gs_log.log'
    # file_path_3 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-14-14:31:25/server/gs_log.log'
    # file_path_4 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-14-12:53:02/server/gs_log.log'
    
    # MMAP
    file_path_1 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-14-23:47:05/server/gs_log.log'
    # Buffer Pool overall
    file_path_2 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-14-23:32:16/server/gs_log.log'
    # Buffer Pool get
    file_path_3 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-14-23:13:41/server/gs_log.log'
    # Buffer Pool Decode
    file_path_4 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-14-22:52:07/server/gs_log.log'
    
    query_ids_1, latency_1_0, latency_1_1 = read_log(file_path_1)
    query_ids_2, latency_2_0, latency_2_1 = read_log(file_path_2)
    query_ids_3, latency_3_0, latency_3_1 = read_log(file_path_3)
    query_ids_4, latency_4_0, latency_4_1 = read_log(file_path_4)

    # assert (np.array(query_ids_ov) == np.array(query_ids_gbp)).all()
    
    per = (np.array(latency_1_0[int(len(latency_1_0)/2):]) - np.array(latency_2_0[int(len(latency_1_0)/2):])) / np.array(latency_1_0[int(len(latency_1_0)/2):])
    print("P99:", np.percentile(np.array(latency_1_0[int(len(latency_1_0)/2):])/2.5, 99))
    
    print('work finished\n')


