
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

def plot_figure_1(index, legends, xticks, data):
    # 生成x轴的坐标
    bar_width=0.2
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(data)
    print(index[0])
    colors = ['red', 'green', 'blue', 'brown']

    for idx in range(data.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], np.array(data[idx, :]), width=bar_width, color='#629a90', zorder=100)


    # ax1.legend(legends)
    plt.xticks(range(0, 3), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Memory/Working_Set (%)')
    plt.ylabel('KQPS')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30-1.pdf', bbox_inches='tight')
    plt.savefig('figs/fig30-1.svg', bbox_inches='tight')


def plot_figure_2(index, legends, xticks, data):
    # 生成x轴的坐标
    bar_width=0.3
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(data)
    print(index[0])
    edgecolors = ['#629a90', '#c4abd0']
    hatchs = ["/////", "\\\\\\\\"]

    for idx in range(data.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], np.array(data[idx, :]), width=bar_width, color=edgecolors[idx], zorder=100)


    ax1.legend(legends)
    plt.xticks(range(1, 4), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('P50 Latency (ms)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30-2.pdf', bbox_inches='tight')
    
if __name__ == "__main__":
    legends = ('IOE-Order', 'Degree-Order', 'createDate-Order')
    xticks = ['MMAP', 'BP w/o LC', 'BP w/ LC']
    data = np.array([[5180, 6016, 6500]])
    plot_figure_1((range(0, 3), [0]), legends, xticks, data)
    
    legends = ('Ratio of New Data = 0', 'Ratio of New Data = 10%', 'createDate-Order')
    xticks = ['3', '10', '14']
    data = np.array([[220, 62, 60],[1090, 115, 255],])
    plot_figure_2((range(1, 4), [-0.15,0.15]), legends, xticks, data)
    
    print('work finished\n')
