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
        
def plot_figure(datas):

    set_mpl()
    fig = plt.figure()
    ax1 = fig.subplots()
    point_num = 100
    x_data = [1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    labels = ['TriCache (10%)','BufferPool (10%)','TriCache (50%)','BufferPool (50%)']
    
    for fig_id in [3, 2, 1, 0]:
        # hist, bin_edges = np.histogram(datas[:, fig_id], point_num)
        if fig_id % 2 == 0:
            line_style = '--'
        else:
            line_style = '-'
            
        if fig_id > 1:
            line_color = plt.get_cmap('tab10')(2)
        else:
            line_color = plt.get_cmap('tab10')(0)

        ax1.plot(x_data, datas[fig_id], color=line_color, linestyle=line_style, label=labels[fig_id])
    plt.legend(loc='upper left')
    plt.grid(True, linestyle='--', color='gray', linewidth=0.5)

    
    ax1.set_xlim(0, 50)
    plt.xticks(range(0, 51, 10), range(0, 51, 10))
    ax1.set_ylim(0, 5)
    plt.yticks(range(0, 5, 1), range(0, 5, 1))
    
    plt.xlabel('Number of Worker')
    plt.ylabel('Worker Throughput (GB)')
    
    plt.savefig('figs/fig12-1.pdf', bbox_inches='tight')

def print_percentile(data):
    print(f"P20:\t{np.percentile(data, 20)}")
    print(f"P30:\t{np.percentile(data, 30)}")
    print(f"P50:\t{np.percentile(data, 50)}")
    print(f"P80:\t{np.percentile(data, 80)}")
    print(f"P90:\t{np.percentile(data, 90)}")
    print(f"P99:\t{np.percentile(data, 99)}")
    print(f"P99.9:\t{np.percentile(data, 99.9)}")

if __name__ == "__main__":
    datas = [[0.064, 0.127, 0.19, 0.25, 0.31, 0.598, 0.858, 1.097, 1.31, 1.47, 1.64, 1.77, 1.91, 2],
             [0.06168, 0.122616, 0.184418, 0.241875, 0.301136, 0.582977, 0.849174, 1.084782, 1.306885, 1.526684, 1.703968, 1.887852, 2.035393, 2.14777],
             [0.093, 0.225, 0.31, 0.446, 0.54, 1.05, 1.53, 1.96, 2.36, 2.7, 2.92, 3.22, 3.4, 3.68],
             [0.113998, 0.22661, 0.353241, 0.4492, 0.557194, 1.08112, 1.638588, 2.01952, 2.414558, 2.945293, 3.158821, 3.518555, 3.958729, 4.036266]]
    
    
    plot_figure(np.array(datas))
    print('\nwork finished')


