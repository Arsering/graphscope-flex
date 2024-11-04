#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 16:44:14 2023
@author: yz
"""

import os
import sys
import re
import json
import numpy as np
from matplotlib import pyplot as plt
import matplotlib as mpl
from concurrent.futures import ProcessPoolExecutor
import pickle

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
            # 'figure.figsize': (10.0, 10 * 0.618),
            'savefig.dpi': 1000,
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
        data.append(int(line))
        count += 1
        line = f.readline()
    return count
def analyze_data(data):
    max_id = max(data)
    counts = np.zeros(max_id+1)
    
    for item in data:
        counts[item] += 1
    
    return [x for x in counts if x != 0]

def plot_figure(data):

    set_mpl()
    fig = plt.figure()
    ax1 = fig.subplots()
    
    # data = np.random.rand(5, 10)
    for fig_id in [10]:
        plt.plot(range(0, data.shape[1]), data[fig_id, :])
    print(sum(x > 1 for x in data[fig_id, :]))
    # plt.yticks(range(0, 13, 2))
    # plt.set_xlim(0, data.shape[1])
    # plt.xticks(range(0, 1500, 300), range(0, 1500, 300))
    # plt.set_ylim(0, data.shape[0])
    # plt.yticks(range(0, 30, 5), range(0, 30, 5))
    
    # plt.xlabel('Number of Access')
    # plt.ylabel('PDF of Person Vertex (%)')
    
    plt.savefig('figs/fig16.pdf', bbox_inches='tight')

def print_percentile(data):
    print(f"P20:\t{np.percentile(data, 20)}")
    print(f"P30:\t{np.percentile(data, 30)}")
    print(f"P50:\t{np.percentile(data, 50)}")
    print(f"P80:\t{np.percentile(data, 80)}")
    print(f"P90:\t{np.percentile(data, 90)}")
    print(f"P99:\t{np.percentile(data, 99)}")
    print(f"P99.9:\t{np.percentile(data, 99.9)}")

def process_file(file_path, time_stamps, slot_num, fd_to_minfd, max_file_page_ids):
    file_page_nums = [0] + list(np.cumsum(np.array(max_file_page_ids)+1))

    results = np.zeros([slot_num, file_page_nums[-1]])
    slot_size = (time_stamps[1] - time_stamps[0]) / slot_num + 2
    max_time_stamp_cur = time_stamps[0] + slot_size
    slot_id_cur = 0
    
    f = open(file_path , 'r')
    line = f.readline()
    while line:
        logs = line.split()
        if logs[1] != 'start':
            if int(logs[0]) > max_time_stamp_cur:
                max_time_stamp_cur += slot_size
                slot_id_cur += 1
            logs_1 = logs[2].split('.')
            results[slot_id_cur][file_page_nums[fd_to_minfd[int(logs_1[0])]]+int(logs_1[1])] += 1 
        line = f.readline()

    return results

def process_wrapper(args):
    return process_file(*args)

def compute(dir_path):
    data = []
    file_paths = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            # 获取文件的完整路径
            if "thread_log" in file:
                numbers = re.findall(r'\d+', file)
                if int(numbers[0]) ==0:
                    continue
                file_path = os.path.join(root, file)
                file_paths.append(file_path)

    fd_count = 0
    fd_to_minfd = {}
    max_file_page_ids = []
    time_stamps = [sys.maxsize, 0]
    for file_path in file_paths:
        print(file_path)
        f = open(file_path , 'r')
        line = f.readline()
        while line:
            logs = line.split()
            if logs[1] != 'start':
                if int(logs[0]) > time_stamps[1]:
                    time_stamps[1] = int(logs[0])
                if int(logs[0]) < time_stamps[0]:
                    time_stamps[0] = int(logs[0])
                for idx in [1, 2]:
                    logs_1 = logs[idx].split('.')
                    if int(logs_1[0]) != 8191:
                        if int(logs_1[0]) not in fd_to_minfd:
                            fd_to_minfd[int(logs_1[0])] = fd_count
                            fd_count += 1
                            max_file_page_ids.append(0)
                            
                        if int(logs_1[1]) > max_file_page_ids[fd_to_minfd[int(logs_1[0])]]:
                            max_file_page_ids[fd_to_minfd[int(logs_1[0])]] = int(logs_1[1])
                    
            line = f.readline()

    tasks = []
    slot_num = 1024*4
    for file_path in file_paths:
        # process_file(file_path, time_stamps, slot_num, fd_to_minfd, max_file_page_ids)
        tasks.append((file_path, time_stamps, slot_num, fd_to_minfd, max_file_page_ids))

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_wrapper, tasks[0:20]))

    results_final = results[0]
    for id in range(1, len(results)):
        results_final += results[id]
    
    data = {
        'results_final': results_final,
        'fd_to_minfd': fd_to_minfd,
        'max_file_page_ids': max_file_page_ids
    }
    os.makedirs(f'figs/fig_16', exist_ok=True)
    with open(f'figs/fig_16/data.pkl','wb') as f:
        pickle.dump(data, f)
        
        
if __name__ == "__main__":
    dir_path = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-09-28-19:10:06/server/graphscope_logs'
    
    with open(f'figs/fig_16/data.pkl','rb') as f:
        data = pickle.load(f)
    results_final = data['results_final']
    fd_to_minfd = data['fd_to_minfd']
    max_file_page_ids = data['max_file_page_ids']

    file_page_nums = [0] + list(np.cumsum(np.array(max_file_page_ids)+1))

    print(sum(results_final[:, file_page_nums[fd_to_minfd[169]]:file_page_nums[fd_to_minfd[169]+1]]))
    plot_figure(results_final[:, file_page_nums[fd_to_minfd[169]]:file_page_nums[fd_to_minfd[169]+1]])
    print('\nwork finished')


