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
    breakdown = np.array([[7.143, 1.023],
                          [7.09, 1.025],
                          [6.862, 0.991]])
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
    plt.xticks(range(1, 3), xticks, fontsize=20)
    
    plt.ylim((0, 8))
    plt.yticks(np.array(range(0, 8, 2)), np.array(range(0, 8, 2)))

    plt.xlabel('Memory/Working_Set (%)')
    plt.ylabel('KQPS')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig21-1.pdf', bbox_inches='tight')
    
def plot_figure_2(index, legends, xticks):
    breakdown = np.array([[265, 2507],
                          [268, 2497.8],
                          [282, 2587]])
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
    plt.xticks(range(1, breakdown.shape[0]), xticks, fontsize=20)
    # plt.ylim((0, 2800))
    # plt.yticks(np.array(range(0, 2600, 500)), np.array(range(0, 2600, 500)))
    

    plt.xlabel('Memory/Working_Set (%)')
    plt.ylabel('SSD IO (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig21-2.pdf', bbox_inches='tight')

def plot_figure_3(index, legends, xticks):
    breakdown = np.array([[117,	238, 364, 477, 588,	720, 840, 958],
                          [119,	155, 229, 309, 397, 467, 535, 618]])
    breakdown = np.array(breakdown) / [1, 2, 3, 4, 5, 6, 7, 8]
    print(breakdown)
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
    plt.xticks(range(1, 9), xticks, fontsize=20)
    
    plt.ylim((0, 155))
    plt.yticks(np.array(range(0, 130, 40)), np.array(range(0, 130, 40)))

    plt.xlabel('Page Number')
    plt.ylabel('Latency per Page (us)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig21-3.pdf', bbox_inches='tight')
    
def plot_figure_4(index, legends, xticks):
    breakdown = np.array([[1.914856, 1.914856, 1.914856, 1.914856, 1.914856, 1.914856, 1.914856, 1.914856],
                          [1.914333, 2.940948, 2.940948, 2.940948, 2.940948, 2.940948, 2.940948, 2.940948]])
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
    plt.xticks(range(1, 9), xticks, fontsize=20)
    plt.ylim((0, 4))
    plt.yticks(np.array(range(0, 4, 1)), np.array(range(0, 4, 1)))
    
    plt.xlabel('Page Number')
    plt.ylabel('SSD Throughput (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig21-4.pdf', bbox_inches='tight')


def plot_figure_5(index, legends, xticks):
    breakdown = np.array([[7.152, 1.023],
                          [7.391, 1.127]])

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
    plt.xticks(range(1, 3), xticks, fontsize=20)
    
    # plt.ylim((0, 155))
    # plt.yticks(np.array(range(0, 130, 40)), np.array(range(0, 130, 40)))

    plt.xlabel('Memory/Working_Set (%)')
    plt.ylabel('KQPS')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig21-5.pdf', bbox_inches='tight')
    
def plot_figure_6(index, legends, xticks):
    breakdown = np.array([[1.914856, 1.914856, 1.914856, 1.914856, 1.914856, 1.914856, 1.914856, 1.914856],
                          [1.914333, 2.940948, 2.940948, 2.940948, 2.940948, 2.940948, 2.940948, 2.940948]])
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
    plt.xticks(range(1, 9), xticks, fontsize=20)
    plt.ylim((0, 4))
    plt.yticks(np.array(range(0, 4, 1)), np.array(range(0, 4, 1)))
    
    plt.xlabel('Page Number')
    plt.ylabel('SSD Throughput (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig21-4.pdf', bbox_inches='tight')
    
if __name__ == "__main__":
    legends = ('IOE-Order', 'Degree-Order', 'createDate-Order')
    xticks = ['30', '10']

    plot_figure_1((range(1, 3), [-0.2, 0, 0.2]), legends, xticks)
    plot_figure_2((range(1, 3), [-0.2, 0, 0.2]), legends, xticks)
    
    legends = ('Random SSD IO', 'Sequential SSD IO')
    xticks = ['1', '2', '3', '4', '5', '6', '7', '8']

    plot_figure_3((range(1, 9), [-0.1, 0.1]), legends, xticks)
    plot_figure_4((range(1, 9), [-0.1, 0.1]), legends, xticks)
    
    legends = ('Random SSD IO', 'Sequential SSD IO')
    xticks = ['30', '10']

    plot_figure_5((range(1, 3), [-0.1, 0.1]), legends, xticks)

    
    print('work finished\n')


