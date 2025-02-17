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

def plot_figure(bar_width,index, legends, xticks, data):
    # 生成x轴的坐标
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()
    
    edgecolors = ['#629a90', '#c4abd0', '#89a7d0', '#ab90c4']
    hatchs = ["/////", "\\\\\\\\", '+++', 'xxx']

    for idx in range(data.shape[0]):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], np.array(data[idx, :])/np.array(data[0, :]), width=bar_width, color='white', edgecolor=edgecolors[idx], hatch=hatchs[idx], linewidth=2, zorder=100)


    ax1.legend(legends, loc='upper center', bbox_to_anchor=(0.5, 1.21), ncol=data.shape[0])
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    # plt.ylim((0, 70))
    # plt.yticks(np.array(range(0, 70, 20)), np.array(range(0, 70, 20)))

    plt.xlabel('Complex Read ID')
    plt.ylabel('P50 Latency (Normalized)')
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig30.svg', bbox_inches='tight')
    plt.savefig('figs/fig30.pdf', bbox_inches='tight')
    
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
    

    # data_p50 = np.array([[53633722, 81839574, 208675860, 11068270, 7673878008, 348118654, 10637920, 947066111, 9161732014, 168553519, 10512108, 149040022, 4993587, 4481718671],
    #                      [29891312, 61445279, 240267886, 9639451, 4019106787, 115863443, 3150591, 234486715, 6304449222, 116668398, 8002525, 263127919, 3847105, 978857611], # old
    #                 [26606799, 59752086, 211838079, 9130781, 3538611701, 94347872, 3065486, 197119644, 6040647534, 112074532, 7094761, 256149658, 3530652, 829786312],
    #                 [19986843, 43012324, 207881241, 5568713, 2195257229, 231266372, 1929366, 83868274, 5521697522, 83639685, 5439273, 415504911, 2757848, 461834885]]) # new
    
    # data_p99 = np.array([[3801281681, 119812564, 1076717397, 22593241, 9437865043, 1405757662, 63880113, 3387964414, 10075016365, 312395513, 15069815, 561014629, 21776821, 66895249928],
    #                      [3341405562, 82696269, 728463413, 17919818, 5502774628, 435443089, 29668895, 839981850, 6902266640, 153923441, 12526381, 433136480, 17890430, 14973327442], # old
    #                 [3043447018, 81599501, 650787546, 17120594, 4860480604, 316139433, 26832877, 646356094, 6583604393, 147798862, 11639650, 422432681, 16822285, 12622085815],
    #                 [2656360343, 59269640, 592520636, 12020456, 3074290650, 368083936, 5866636, 221170951, 6078087124, 130638325, 9916597, 712311932, 14113966, 6823301411]]) # new
    # legends = ('A', 'B', 'C', 'D')
    # plot_figure(0.2, (range(1, 15), [-0.3,-0.1, 0.1, 0.3]), legends, xticks, data_p99)
    
    data_p50 = np.array([[14379730, 24970068, 120325475, 4418215, 1256590093, 50289351, 1651298, 43761841, 4386566468, 59669895, 3401583, 157207034, 1806317, 130499425],
                         [14403479, 23529888, 120710342, 4428173, 1259351426, 49442156, 1646526, 43787524, 4297895828, 59586397, 3391162, 156901091, 1815037, 130980359]])
    data_p99 = np.array([[14097731, 24781866, 118548559, 4359400, 1208457997, 45163753, 1649630, 43171657, 4358227542, 58234521, 3263770, 134371997, 1753003, 108294495],
                         [14335271, 27116904, 125264448, 5802585, 1191746685, 49644660, 1762531, 45189985, 5347211909, 106511000, 3410929, 158131375, 1821333, 131611450]])
    plot_figure(0.3, (range(1, 15), [-0.15,0.15]), legends, xticks, data_p99)
