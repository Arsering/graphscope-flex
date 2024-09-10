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
    mark = 0
    while line:
        if 'IC1; mean' in line and mark == 1:
            
        data.append(int(line))
        count += 1
        line = f.readline()
    return count

if __name__ == "__main__":
    dir_path = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-09-05-12:37:01/server/graphscope_logs'
    
    data = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            # 获取文件的完整路径
            if "thread_log" in file:
                numbers = re.findall(r'\d+', file)
                if int(numbers[0]) > 20:
                    continue
                print(file, end='\t')
                file_path = os.path.join(root, file)
                count = read_data(file_path, data)
                print(count)

    counts = analyze_data(data)
    plot_figure(counts)
    print('\nwork finished')


