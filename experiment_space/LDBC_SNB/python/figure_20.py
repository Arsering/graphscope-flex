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
    breakdown = np.array([[4.57795805, 3.92483911, 1.31494287, 1.08432262, 1.17343261, 2.73677105,
 3.92751506, 4.84783995, 3.78131818, 1.19562744, 3.7339611,  2.0176873,
 5.47644121, 8.56821883, 2.02390644, 4.60040182, 1.37308052, 1.25613197,
 1.1644287,  1.98959802, 0.83135226],
                          [1.47332986, 2.59176026,  5.89430887,  2.19341595,  3.6410134,   5.62957765,
  2.72303991,  6.05820411,  1.64069128,  2.65376036,  2.38519574,  6.53852279,
  2.09142804, 11.12000254,  4.79077926,  2.18528238,  1.26516651,  1.1038026,
  1.22113265,  0.99553719, 0.57402764],
                          [1.15892553, 4.32010125, 0.9003218,  2.18952938, 1.20249642, 1.27618588,
 2.30068545, 1.78272666, 2.19794726, 1.89321284, 1.20870973, 2.58054691,
 1.20523481, 1.56645315, 1.2363188,  2.35079552, 1.13713518, 1.1301883,
 1.08567996, 0.99762206, 0.59344838]])
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
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :14], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)


    ax1.legend(legends,loc='upper left')
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    plt.ylim((0, 12))
    plt.yticks([0, 1, 4, 8], [0, 1, 4, 8])

    plt.xlabel('Complex Read ID')
    plt.ylabel('Acceleration Ratio')
    # (Normalized by LDBC-Native-Order)
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig20-1.pdf', bbox_inches='tight')
    
def plot_figure_2(index, legends, xticks):
    breakdown = np.array([[3.868749, 3.64391134, 1.58799454, 1.17091039, 2.43906702, 3.20366431,
            3.13655053, 2.84212218, 3.41739048, 1.35096986, 3.90447287, 2.45854866,
            4.99794949, 8.65071551, 2.43691192, 2.83721302, 1.45988632, 1.01062307,
            0.98842607, 1.35481579, 0.62697663],
                        [1.30422671, 2.3461495, 5.21815858, 2.10608048, 2.1684751, 4.01780825,
            2.49840027, 5.85689902, 1.49026537, 2.4625371, 1.93877819, 5.61294562,
            1.74099044, 8.79748724, 3.96000619, 2.00833573, 1.19700961, 1.05623057,
            1.12739123, 0.94352727, 0.58344467],
                        [1.14853519, 4.00664533, 0.88919694, 2.14605667, 1.16899148, 1.19739947,
            2.23897055, 1.78610094, 2.07556932, 1.87841364, 1.20313376, 2.56958149,
            1.20195425, 1.56272719, 1.23845081, 2.26626954, 1.11659206, 1.02039894,
            1.02693854, 1.02109813, 0.68605373]])

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
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :14], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)


    ax1.legend(legends,loc='upper left')
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    plt.ylim((0, 10))
    plt.yticks([0, 1, 4, 8], [0, 1, 4, 8])

    plt.xlabel('Complex Read ID')
    plt.ylabel('Acceleration Ratio')
    # (Normalized by LDBC-Native-Order)
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig20-2.pdf', bbox_inches='tight')
    
def plot_figure_3(index, legends, xticks):
    breakdown = np.array([[1.37148403, 2.42075423, 5.46465445, 2.16028226, 2.43360885, 4.40741216,
 2.59275775, 5.65525699, 1.54141351, 2.54040613, 2.09364955, 5.9401689,
 1.86402179, 9.39652858, 4.2157354, 2.07689234, 1.22240907, 1.0629137,
 1.18338852, 0.96472651, 0.56998444],
                        [1.33109482, 2.37828413, 5.20015268, 2.09461418, 2.26590741, 4.11223833,
 2.56038521, 5.82670951, 1.57636038, 2.46481012, 2.01761328, 5.70422681,
 1.79463039, 9.10297053, 3.89149891, 2.03734138, 1.21926328, 1.05132384,
 1.15618635, 0.95325909, 0.57303124],
                        [1.30422671, 2.3461495, 5.21815858, 2.10608048, 2.1684751,  4.01780825,
 2.49840027, 5.85689902, 1.49026537, 2.4625371, 1.93877819, 5.61294562,
 1.74099044, 8.79748724, 3.96000619, 2.00833573, 1.19700961, 1.05623057,
 1.12739123, 0.94352727, 0.58344467]])
    legends = ('Update Frequency (x1)', 'Update Frequency (x2)', 'Update Frequency (x4)')

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
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :14], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)


    ax1.legend(legends,loc='upper left')
    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    plt.ylim((0, 10))
    plt.yticks([0, 1, 4, 8], [0, 1, 4, 8])

    plt.xlabel('Complex Read ID')
    plt.ylabel('Speedup')

    # (Normalized by LDBC-Native-Order)
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig20-3.pdf', bbox_inches='tight')

def plot_figure_5(index, legends, xticks):
    breakdown = np.array([[1.14082901, 1.04010146, 1.13697212, 1.03960826, 2.06289143, 1.19248983,
                    1.08209938, 1.03735031, 1.15840121, 1.08651032, 1.21209305, 1.12193665,
                    1.1863258,  1.20945315, 1.20842404, 1.03998346, 1.04978936, 1.05013651,
                    1.04824821, 1.04537538, 1.01703569],
                        [1.33109482, 2.37828413, 5.20015268, 2.09461418, 2.26590741, 4.11223833,
                    2.56038521, 5.82670951, 1.57636038, 2.46481012, 2.01761328, 5.70422681,
                    1.79463039, 9.10297053, 3.89149891, 2.03734138, 1.21926328, 1.05132384,
                    1.15618635, 0.95325909, 0.57303124],
                        [1.30422671, 2.3461495, 5.21815858, 2.10608048, 2.1684751,  4.01780825,
                    2.49840027, 5.85689902, 1.49026537, 2.4625371, 1.93877819, 5.61294562,
                    1.74099044, 8.79748724, 3.96000619, 2.00833573, 1.19700961, 1.05623057,
                    1.12739123, 0.94352727, 0.58344467]])

    # 生成x轴的坐标
    bar_width=0.2
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(breakdown)
    print(index[0])
    colors = ['red', 'green', 'blue', 'brown']

    for idx in range(1):
        print(idx)
        ax1.bar(np.array(index[0])+index[1][idx], breakdown[idx, :14], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)


    plt.xticks(range(1, 15), xticks, fontsize=20)
    
    plt.ylim((0, 2.5))
    plt.yticks([0, 1, 1.5, 2], [0, 1, 1.5, 2])

    plt.xlabel('Complex Read ID')
    plt.ylabel('Speedup')

    # (Normalized by LDBC-Native-Order)
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig20-5.pdf', bbox_inches='tight')
    
def plot_figure_4(index, legends, xticks):
    breakdown = np.array([[4.57795805, 3.92483911, 1.31494287, 1.08432262, 1.17343261, 2.73677105,
 3.92751506, 4.84783995, 3.78131818, 1.19562744, 3.7339611,  2.0176873,
 5.47644121, 8.56821883, 2.02390644, 4.60040182, 1.37308052, 1.25613197,
 1.1644287,  1.98959802, 0.83135226],
                          [1.47332986, 2.59176026,  5.89430887,  2.19341595,  3.6410134,   5.62957765,
  2.72303991,  6.05820411,  1.64069128,  2.65376036,  2.38519574,  6.53852279,
  2.09142804, 11.12000254,  4.79077926,  2.18528238,  1.26516651,  1.1038026,
  1.22113265,  0.99553719, 0.57402764],
                          [1.15892553, 4.32010125, 0.9003218,  2.18952938, 1.20249642, 1.27618588,
 2.30068545, 1.78272666, 2.19794726, 1.89321284, 1.20870973, 2.58054691,
 1.20523481, 1.56645315, 1.2363188,  2.35079552, 1.13713518, 1.1301883,
 1.08567996, 0.99762206, 0.59344838]])
    # 生成x轴的坐标
    bar_width=0.2
    set_mpl()

    fig = plt.figure()
    ax1 = fig.subplots()

    print(index[0])
    colors = ['red', 'green', 'blue', 'brown']
    index_tmp = [1, 8]
    for idx in [1]:
        print(idx)
        print(index)
        ax1.bar(np.array(range(len(index_tmp))), breakdown[idx, index_tmp], width=bar_width, color=plt.get_cmap('tab10')(idx), zorder=100)

    # ax1.legend(legends,loc='upper left')
    plt.xticks(range(len(index_tmp)), np.array(xticks)[index_tmp], fontsize=20)
    
    plt.ylim((0, 12))
    plt.yticks([0, 1, 4, 8], [0, 1, 4, 8])

    plt.xlabel('Complex Read ID')
    plt.ylabel('Speedup')
    # (Normalized by LDBC-Native-Order)
    plt.grid(True, linestyle='--', linewidth=0.5, zorder=0)

    # 显示图形
    plt.savefig('figs/fig20-4.pdf', bbox_inches='tight')
    
def data_analysis_inner(file_path, datas, count):
    f = open(file_path , 'r')
    line = f.readline()
    
    while line:
        logs = line.split(" ")
        datas[count, 0] = int(logs[0])
        datas[count, 1] = int(logs[1])
        datas[count, 2] = int(logs[2])
        count += 1
        line = f.readline()
    return count
def data_analysis(dir_path):
    datas = np.zeros([3000000, 3])

    count = 0
    for root, dirs, files in os.walk(dir_path):
            for file in files:
                if "thread_log" not in file:
                    continue
                file_path = os.path.join(root, file)
                count = data_analysis_inner(file_path, datas, count)
    print(count)
    print(np.mean(datas[0:count, 0] - datas[0:count, 1])/2.7/1000)
    count_tmp = 0
    latency = 0
    datas_sum = np.zeros([30, 2])
    for id in range(0, count):
        datas_sum[int(datas[id, 2])][0] += datas[id, 0] - datas[id, 1]
        datas_sum[int(datas[id, 2])][1] += 1
    return (datas_sum[1:22, 0]/2.7/datas_sum[1:22, 1]/1000)

                
if __name__ == "__main__":
    data_dir_paths_1 = ['/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-09:47:28/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-10:05:22/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-01:24:50/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-08:52:01/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-09:25:17/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-00:29:51/server/graphscope_logs']
    
    data_dir_paths_2 = ['/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-27-01:24:49/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-17:24:26/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-17:57:36/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-27-01:15:47/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-17:05:29/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-18:51:09/server/graphscope_logs']
    
    data_dir_paths_3 = ['/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-15:32:18/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-16:08:22/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-14:19:00/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-13:31:57/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-17:24:26/server/graphscope_logs',
                '/data/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-10-26-17:05:29/server/graphscope_logs']
    # result1 = data_analysis(data_dir_paths_3[3])
    # result2 = data_analysis(data_dir_paths_1[4])

    # print(result1/result2)

    legends = ('Memory/Working_Set = 0.5', 'Memory/Working_Set = 0.3', 'Memory/Working_Set = 0.1')
    xticks = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14']
    # plot_figure_1((range(1, 15), [-0.2, 0, 0.2]), legends, xticks)
    # plot_figure_2((range(1, 15), [-0.2, 0, 0.2]), legends, xticks)
    # plot_figure_3((range(1, 15), [-0.2, 0, 0.2]), legends, xticks)
    # legends = ('Memory/Working_Set = 0.3')
    plot_figure_5((range(1, 15), [0]), legends, xticks)
    # plot_figure_4((range(1, 15), [ 0,]), legends, xticks)

    print('work finished\n')


