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
            'figure.figsize': (10.0, 10 * 0.418),
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
    # IC1 IC3 IC10 IC14
    breakdown = np.array([[34572, 12689, 72755, 4068, 1478882, 33461, 2638, 25027, 2053583, 37120, 2977, 77192, 2239, 170297],
                          [34285, 12609, 72541, 4012, 1474326, 31984, 2559, 21489, 2063350, 36852, 2864, 73679, 2268, 164500]])
    # 生成x轴的坐标
    bar_width=0.3
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(breakdown)
    print(index[0])
    edgecolors = ['#629a90', '#c4abd0']
    hatchs = ["/////", "\\\\\\\\"]

    for idx in range(breakdown.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], np.array(breakdown[idx, :])/1000, width=bar_width, color='white', edgecolor=edgecolors[idx], hatch=hatchs[idx], linewidth=1, zorder=100)


    # ax1.legend(legends)
    # plt.xticks(range(1, 4), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('Mean Latency (ms)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30-2.pdf', bbox_inches='tight')

def plot_figure_2(index, legends, xticks):
    # IC1 IC3 IC10 IC14
    breakdown = np.array([[34285, 12609, 72541, 4012, 1474326, 31984, 2559, 21489, 2063350, 36852, 2864, 73679, 2268, 164500],
                          [34572, 12689, 72755, 4068, 1478882, 33461, 2638, 25027, 2053583, 37120, 2977, 77192, 2239, 170297]])
    # 生成x轴的坐标
    bar_width=0.3
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(breakdown)
    print(index[0])
    edgecolors = ['#629a90', '#c4abd0']
    hatchs = ["/////", "\\\\\\\\"]

    for idx in range(breakdown.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], np.array(breakdown[idx, :])/1000, width=bar_width, color='white', edgecolor=edgecolors[idx], hatch=hatchs[idx], linewidth=2, zorder=100)


    ax1.legend(legends, loc='upper left')
    # plt.xticks(range(1, 4), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('Mean Latency (ms)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30-2.pdf', bbox_inches='tight')
    
if __name__ == "__main__":
    legends = ('IOE-Order', 'Degree-Order', 'createDate-Order')
    xticks = ['100', '80', '50', '20']

    # plot_figure_1((range(1, 5), [0.15,-0.15]), legends, xticks)
    # print('work finished\n')
    
    legends = ('Append-Only', 'Online-Grouping')
    xticks = ['3', '10', '14']
    plot_figure_2((range(1, 15), [-0.15,0.15]), legends, xticks)



