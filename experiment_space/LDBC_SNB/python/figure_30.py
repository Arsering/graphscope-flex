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
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('Mean Latency (ms)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30-2.pdf', bbox_inches='tight')

def plot_figure_3(index, legends, xticks):
    # IC1 IC3 IC10 IC14
    breakdown = np.array([[30271118, 60845057, 713727161, 9140770, 3932780873, 183643619, 3617707, 261554993, 6892238358, 146005012, 8058391, 862633062, 3903388, 1342211075],
                          [29427197, 63009886, 674801982, 12586068, 4210675091, 168560095, 3753670, 253615552, 7988715760, 231659523, 7797018, 803951910, 3780296, 1291325529]])
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
        ax1.bar(np.array(index[0])+index[1][idx], np.array(breakdown[idx, :])/np.array(breakdown[0, :]), width=bar_width, color='white', edgecolor=edgecolors[idx], hatch=hatchs[idx], linewidth=2, zorder=100)


    ax1.legend(legends, loc='upper left')
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('Mean Latency (ms)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30-3.pdf', bbox_inches='tight')


def plot_figure_4(index, legends, xticks):
    # IC1 IC3 IC10 IC14
    breakdown = np.array([[2925091329, 83052254, 1778433226, 18564849, 5486507355, 676027225, 32317745, 677845258, 8260803831, 231884711, 12987839, 1513325164, 18090742, 19122230282], # old
                          [2911236714, 86010183, 1687061504, 22747350, 5859243126, 646000432, 32185901, 663146571, 9561675844, 334077555, 12690720, 1420586638, 17536957, 18339661802]]) # new
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
        ax1.bar(np.array(index[0])+index[1][idx], np.array(breakdown[idx, :])/np.array(breakdown[0, :]), width=bar_width, color='white', edgecolor=edgecolors[idx], hatch=hatchs[idx], linewidth=2, zorder=100)


    ax1.legend(legends, loc='upper left')
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('Mean Latency (ms)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30-4.pdf', bbox_inches='tight')

def plot_figure(index, legends, xticks, data):
    # 生成x轴的坐标
    bar_width=0.3
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()
    
    edgecolors = ['#629a90', '#c4abd0']
    hatchs = ["/////", "\\\\\\\\"]

    for idx in range(data.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], np.array(data[idx, :])/np.array(data[0, :]), width=bar_width, color='white', edgecolor=edgecolors[idx], hatch=hatchs[idx], linewidth=2, zorder=100)


    ax1.legend(legends, loc='upper center', bbox_to_anchor=(0.5, 1.21), ncol=2)
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('Mean Latency (ms)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30.svg', bbox_inches='tight')
    
    
if __name__ == "__main__":
    legends = ('IOE-Order', 'Degree-Order', 'createDate-Order')
    xticks = ['100', '80', '50', '20']

    # plot_figure_1((range(1, 5), [0.15,-0.15]), legends, xticks)
    # print('work finished\n')
    
    legends = ('Append-Only', 'Online-Grouping')
    xticks = range(1, 15)
    # plot_figure_2((range(1, 15), [-0.15,0.15]), legends, xticks)

    legends = ('Without', 'With')
    # plot_figure_3((range(1, 15), [-0.15,0.15]), legends, xticks)
    # plot_figure_4((range(1, 15), [-0.15,0.15]), legends, xticks)
    

    data_p50 = np.array([[34893205, 34282811, 743591103, 8773625, 4869594308, 184025075, 1994497, 83412545, 5914860173, 151925138, 8784343, 553662104, 4281773, 725667779], # old
                    [33364735, 34393506, 686205371, 9360477, 4782691424, 257686791, 2035463, 78140433, 6286026997, 179707152, 8394813, 507030913, 4093609, 776954756]]) # new
    data_p99 = np.array([[3235027770, 51805024, 1889054635, 18239192, 6081731592, 716405585, 5236673, 207997068, 7182583417, 234800212, 13376454, 962980483, 19155616, 10334742336], # old
                    [3175933767, 51405919, 1696578155, 19406632, 5996399104, 1144534764, 5352627, 191316526, 7603186509, 272136661, 12800288, 888940108, 18221645, 10698544758]]) # new
    plot_figure((range(1, 15), [-0.15,0.15]), legends, xticks, data_p99)