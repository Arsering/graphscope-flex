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

def plot_figure_1(index, legends, xticks):
    breakdown = np.array([[66183, 5452, 1483],
                          [14681, 6949, 1855]])
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


    ax1.legend(legends)
    plt.xticks(range(1, 4), xticks, fontsize=20)
    
    plt.ylim((0, 16000))
    plt.yticks(np.array(range(0, 15000, 3000)), np.array(range(0, 15, 3)))

    plt.xlabel('Memroy/Working_Set (%)')
    plt.ylabel('KQPS')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig18-1.pdf', bbox_inches='tight')
    
def plot_figure_2(index, legends, xticks):
    breakdown = np.array([[0, 407.63, 1648.28],
                          [0, 244.96, 1314.34]])
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

    ax1.legend(legends,loc='upper left')
    plt.xticks(range(1, 4), xticks, fontsize=20)
    plt.ylim((0, 1800))
    plt.yticks(np.array(range(0, 1700, 400)), np.array(range(0, 1700, 400)))
    

    plt.xlabel('Memroy/Working_Set (%)')
    plt.ylabel('SSD IO (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig18-2.pdf', bbox_inches='tight')
    
if __name__ == "__main__":
    legends = ('MMAP', 'BufferPool')
    xticks = ['100', '50', '30']

    plot_figure_1((range(1, 4), [-0.1, 0.1]), legends, xticks)
    plot_figure_2((range(1, 4), [-0.1,  0.1]), legends, xticks)

    print('work finished\n')


