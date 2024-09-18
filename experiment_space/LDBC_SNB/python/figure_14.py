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
import pandas as pd
from openpyxl import load_workbook
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
        
def plot_figure(breakdown, index, legends, xticks):
        # 生成x轴的坐标
    bar_width=0.3
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()
    colors = ['red', 'green', 'blue', 'brown']

    for idx in range(breakdown.shape[0]):
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :], width=bar_width, color=plt.get_cmap('tab10')(idx+1), zorder=100)


    ax1.legend(legends, loc='upper left')
    plt.xticks(range(1, 9), xticks, fontsize=20)
    plt.ylim((0, 1000))
    plt.yticks(np.array(range(0, 1000, 200)), np.array(range(0, 1000, 200)))
    plt.xlabel('Number of Page')
    plt.ylabel('Latency (us)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig14.pdf', bbox_inches='tight')

if __name__ == "__main__":
    breakdown = np.array([[117, 238, 364, 477, 588, 720, 840, 958],
                          [119, 155, 229, 309, 397, 467, 535, 618]])

    legends = ('Single-shot', 'Multi-shot')
    xticks = ['1', '2', '3', '4', '5', '6', '7', '8']

    plot_figure(breakdown, (range(1, 9), [-0.15, 0.15]), legends, xticks)
    
    print('work finished\n')
