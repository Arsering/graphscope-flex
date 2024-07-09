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

def plot_figure(breakdown, index, legends, xticks):
        # 生成x轴的坐标
    bar_width=0.2
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(breakdown)
    print(index[0])
    colors = ['red', 'green', 'blue', 'brown']

    for idx in range(breakdown.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)


    ax1.legend(legends, bbox_to_anchor=(1, 0), loc=3)
    plt.xticks(range(1, 8), xticks, fontsize=20)
    plt.ylim((0, 800))
    plt.yticks(np.array(range(0, 900, 200)), np.array(range(0, 900, 200)))
    plt.xlabel('#Partition')
    plt.ylabel('KOPS (SSD)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('fig5.pdf', bbox_inches='tight')

if __name__ == "__main__":
    breakdown = np.array([[0.403, 2.368, 2.37, 2.38, 2.38, 2.385, 2.381],
                          [0.13, 2.49, 2.49, 2.49, 2.49, 2.49, 2.49],
                        [0.21, 2.48, 2.48, 2.48, 2.48, 2.52, 2.52]]) / 4 * 1024 * 1024 /1000

    breakdown = np.array([[0.041, 0.198, 0.48, 1.03, 1.49, 1.87, 2.12],
                          [0.043, 0.244, 0.490, 1.10, 1.66, 2.11, 2.38],
                        [0.042, 0.24, 0.50, 1.11, 1.73, 2.18, 2.45]]) / 4 * 1024 * 1024 /1000

    legends = ('#Worker=150', '#Worker=300', '#Worker=450')
    xticks = ['1', '10', '20', '40', '80', '160', '320']

    plot_figure(breakdown, (range(1, 8), [-0.2, 0,  0.2]), legends, xticks)
    
    print('work finished\n')


