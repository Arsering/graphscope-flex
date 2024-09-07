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

    print(breakdown)
    print(index[0])
    colors = ['red', 'green', 'blue', 'brown']

    for idx in range(breakdown.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)


    # ax1.legend(legends, bbox_to_anchor=(1, 0), loc=3)
    ax1.legend(legends, loc='upper left')
    plt.xticks(range(1, 7), xticks, fontsize=20)
    plt.ylim((0, 6))
    plt.yticks(np.array(range(0, 60, 10))/10, np.array(range(0, 60, 10))/10)
    plt.xlabel('Column Size (Byte)')
    plt.ylabel('Access Throughput (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig8-2.pdf', bbox_inches='tight')
    
def plot_figure1(breakdown, index, legends, xticks):
        # 生成x轴的坐标
    bar_width=0.3
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(breakdown)
    print(index[0])
    colors = ['red', 'green', 'blue', 'brown']

    for idx in range(breakdown.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)


    # ax1.legend(legends, bbox_to_anchor=(1, 0), loc=3)
    ax1.legend(legends, loc='upper left')
    plt.xticks(range(1, 7), xticks, fontsize=20)
    plt.ylim((0, 3))
    plt.yticks(np.array(range(0, 30, 5))/10, np.array(range(0, 30, 5))/10)
    plt.xlabel('String Size (Byte)')
    plt.ylabel('Access Throughput (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig8-3.pdf', bbox_inches='tight')
    

if __name__ == "__main__":
    # columnVSrow, 50% 同时访问
    breakdown = np.array([[0.022, 0.090, 0.361, 1.435, 2.869, 2.831],
                          [0.011, 0.044, 0.180, 0.722, 2.887, 2.857]])
    
    # columnVSrow, 50% 不同时访问，访问概率不一样
    breakdown = np.array([[0.011, 0.045, 0.181, 0.726, 2.907, 2.821],
                          [0.019, 0.089, 0.354, 1.355, 5.328, 4.860]])
    
    # columnVSrow, 50% 不同时访问，访问概率不一样
    breakdown = np.array([[0.011, 0.044, 0.180, 0.723, 2.897, 2.844],
                          [0.005, 0.035, 0.170, 0.702, 2.870, 2.815]])

    legends = ('Contiguously', 'Separately')
    xticks = ['16', '64', '256', '1024', '4096', '16384']

    plot_figure1(breakdown, (range(1, 7), [-0.15, 0.15]), legends, xticks)
    
    print('work finished\n')


