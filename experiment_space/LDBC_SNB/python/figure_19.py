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

def parse_file(file_path):
    f = open(file_path , 'r')
    line = f.readline()
    count = 0 
    
    while line:
        logs = line.split(';')
        if len(logs) > 1 and logs[0] == 'IC1':
            break
        line = f.readline()
    for id in range(0, 14):
        logs = line.split(';')
        sub_logs = logs[1].split(':')
        print(f'IC-{id+1}', end='\t')
        print(round(float(sub_logs[1])/2.7/1000000, 2))
        line = f.readline()


    
if __name__ == "__main__":
    file_path = '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-16-12:10:28/server/gs_log.log'
    parse_file(file_path)

    print('work finished\n')


