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
import pandas as pd
from collections import Counter

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

def plot_figure1(datas):
    percentiles = [90, 92, 94, 96, 98, 100]
    base_latency = np.ones([1, 6])*datas[0, 0]
    print(base_latency)
    datas = np.ones([3, 6]) * 67855
    datas[0] = np.array([67855.0, 78014.1, 87580.3, 100526.8, 114746.6, 127966.0]) # new
    datas[1] = np.array([67855.0, 92578.4, 125912.2, 166406.2, 208521.0, 253497.8])-datas[0] # partial
    datas[2] = np.array([67855.0, 80420.2, 95012.1, 111164.2, 128217.5, 145888.3])-datas[0] # CBI
    datas[0] = datas[0]-base_latency
    datas = datas / 1000
    base_latency = base_latency / 1000
    colors = [1, 2, 3, 4]
    labels = ['Data-Size Increase', 'Partial Ordered', 'Edge-list CBI', 'Dirty-Page Write Back']

    # Create the stacked bar chart
    set_mpl()
    plt.figure()

    # Plot the base latency
    plt.bar(percentiles, base_latency[0, :], color=plt.get_cmap('tab10')(0), label='Baseline', zorder=100)

    # Plot each stack part using a for loop
    bottom = base_latency[0, :]
    for stack, color, label in zip(datas, colors, labels):
        plt.bar(percentiles, stack, bottom=bottom, color=plt.get_cmap('tab10')(color), label=label, zorder=100)
        bottom += np.array(stack)

    # Add labels and title
    plt.xlabel('Percentiles')
    plt.ylabel(f'P50 Latency (ms)')
    plt.legend(loc='upper left')
    plt.title('P50 Latency Comparison of Complex Read 14')

    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    plt.savefig('figs/fig26-1.pdf', bbox_inches='tight')


def plot_figure(datas, percentile_number, query_id):
    percentiles = [0, 20, 40, 60, 80, 100]
    base_latency = np.ones([1, 6])*datas[0, 0]
    print(base_latency)
    datas[1] = datas[1]-datas[0] # partial
    datas[2] = datas[2]-datas[0] # partial
    datas[0] = datas[0]-base_latency
    print(datas)
    colors = [1, 2, 3, 4]
    labels = ['Data-Size Increase', 'Partial Ordered', 'Edge-list CBI'] # , 'Dirty-Page Write Back'

    # Create the stacked bar chart
    set_mpl()
    plt.figure()

    # Plot the base latency
    plt.bar(np.array(percentiles)/20, base_latency[0, :], width=0.5, color=plt.get_cmap('tab10')(0), label='Baseline', zorder=100)

    # Plot each stack part using a for loop
    bottom = base_latency[0, :]
    for stack, color, label in zip(datas, colors, labels):
        plt.bar(np.array(percentiles)/20, stack, bottom=bottom, width=0.5, color=plt.get_cmap('tab10')(color), label=label, zorder=100)
        bottom += np.array(stack)

    # Add labels and title
    plt.xlabel('X Value')
    plt.ylabel(f'P{percentile_number} Latency (ms)')
    
    # plt.legend(loc='upper left')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.25), ncol=2)
    # plt.title(f'P{percentile_number} Latency Comparison of Complex Read {query_id}')
    plt.xticks(range(0, 6), percentiles, fontsize=20)

    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    plt.savefig('figs/fig26-1.pdf', bbox_inches='tight')
    
def data_analysis_inner(file_path, slot_dict):
    f = open(file_path , 'r')
    line = f.readline()
    
    while line:
        logs = line.split()

        if int(logs[0]) != 0:
            line = f.readline()
            continue
        for i in range(0, int(logs[3])):
            slot_dict[int(logs[1])].append(int(logs[1])+i)
        line = f.readline()
        

def data_analysis(dir_path, slot_dict):
    for root, dirs, files in os.walk(dir_path):
            for file in files:
                if "thread_log" not in file:
                    continue
                number = ''.join(filter(str.isdigit, file))
                if int(number) > 30:
                    continue
                file_path = os.path.join(root, file)
                data_analysis_inner(file_path, slot_dict)

if __name__ == "__main__":

    data_dir = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/without_local_cache'
    data_dir = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/with_local_cache'


    slot_dict = {i: [] for i in range(270)} 
    data_analysis(data_dir, slot_dict)
    
    print('\nwork finished')

