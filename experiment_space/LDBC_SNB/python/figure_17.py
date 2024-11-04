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
    breakdown = np.array([[1.63, 1.12, 0.64],
                          [1.4, 1.04, 0.63],
                        [1.74, 1.18, 0.66]])
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
    
    plt.ylim((0, 1.8))
    plt.yticks(np.array(range(0, 18, 5))/10, np.array(range(0, 18, 5))/10)

    plt.xlabel('Memroy/Working_Set')
    plt.ylabel('KOPS (IC-1)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig17-1.pdf', bbox_inches='tight')
    
def plot_figure_2(index, legends, xticks):
    breakdown = np.array([[31.3, 19.1, 10.9],
                          [30.7, 19.1, 10.7],
                        [40.9, 24.5, 12.7]])
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
    plt.ylim((0, 42))
    plt.yticks(np.array(range(0, 42, 10)), np.array(range(0, 42, 10)))
    
    plt.xlabel('Memroy/Working_Set')
    plt.ylabel('KOPS (IC-13)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig17-2.pdf', bbox_inches='tight')
    
def plot_figure_3(index, legends, xticks):
    breakdown = np.array([[193.6, 301.3, 550.4],
                          [212.4, 322.7, 553.0],
                        [174.9, 279.0, 529.7]])
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
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :], width=bar_width, color=plt.get_cmap('tab10')(idx), edgecolor='black', zorder=100)

    ax1.legend(legends, loc='upper left')
    plt.xticks(range(1, 4), xticks, fontsize=20)
    plt.ylim((0, 560))
    plt.yticks(np.array(range(0, 560, 100)), np.array(range(0, 560, 100)))
    
    plt.xlabel('Memroy/Working_Set')
    plt.ylabel('SSD IO (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig17-3.pdf', bbox_inches='tight')
    
def plot_figure_4(index, legends, xticks):
    breakdown = np.array([[15.2, 25.6, 45.7],
                          [15.6, 25.8, 46.3],
                        [11.4, 19.8, 38.7]])
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
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :], width=bar_width, color=plt.get_cmap('tab10')(idx), edgecolor='black', zorder=100)

    ax1.legend(legends, loc='upper left')
    plt.xticks(range(1, 4), xticks, fontsize=20)
    plt.ylim((0, 50))
    plt.yticks(np.array(range(0, 50, 10)), np.array(range(0, 50, 10)))
    
    plt.xlabel('Memroy/Working_Set')
    plt.ylabel('SSD IO (GB)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig17-4.pdf', bbox_inches='tight')

if __name__ == "__main__":
    legends = ('LDBC Order', 'Random Order', 'Degree Order')
    xticks = ['0.5', '0.3', '0.1']

    plot_figure_1((range(1, 4), [-0.2, 0,  0.2]), legends, xticks)
    plot_figure_1((range(1, 4), [-0.2, 0,  0.2]), legends, xticks)

    plot_figure_3((range(1, 4), [-0.2, 0,  0.2]), legends, xticks)
    plot_figure_4((range(1, 4), [-0.2, 0,  0.2]), legends, xticks)

    print('work finished\n')


