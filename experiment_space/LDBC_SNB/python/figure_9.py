#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 16:44:14 2023
@author: yz
"""

import os
import sys
import re
import json
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
        
def read_data(file_path, data):
    f = open(file_path , 'r')
    line = f.readline()
    count = 0 
    
    while line:
        logs = line.split(" ")
        # data.append((int(logs[0]), int(logs[1]), int(logs[2])))
        data.append(int(logs[2]))
        count += 1
        line = f.readline()
    return count

def plot_figure(file_path):
    data = []
    data_name = 'KNOWS'
    with open(file_path, 'r') as file:
        file_data = json.load(file)
        for id_key, id_data in file_data.items():
            if data_name in id_data:
                data.append(id_data[data_name])
            else:
                data.append(0)

    set_mpl()
    fig = plt.figure()
    ax1 = fig.subplots()
    point_num = 100

    for fig_id in [0]:
        hist, bin_edges = np.histogram(data, point_num)
        ax1.plot(np.array(bin_edges[0:point_num]), hist[0:point_num]/sum(hist[0:point_num])*100, color=plt.get_cmap('tab10')(fig_id))
        print_percentile(data)
        ax1.fill_between(np.array(bin_edges[0:point_num]), hist[0:point_num]/sum(hist[0:point_num])*100, alpha=0.5)
    # plt.legend(loc='lower right')
    plt.grid(True, linestyle='--', color='gray', linewidth=0.5)

    
    # ax1.set_xlim(0, 1500)
    # plt.xticks(range(0, 1500, 300), range(0, 1500, 300))
    # ax1.set_ylim(0, 30)
    # plt.yticks(range(0, 30, 5), range(0, 30, 5))
    
    plt.xlabel('Number of Edge')
    plt.ylabel('PDF of Vertex (%)')
    
    plt.savefig('figs/fig9-3.pdf', bbox_inches='tight')

def print_percentile(data):
    print(f"P20:\t{np.percentile(data, 20)}")
    print(f"P30:\t{np.percentile(data, 30)}")
    print(f"P50:\t{np.percentile(data, 50)}")
    print(f"P80:\t{np.percentile(data, 80)}")
    print(f"P90:\t{np.percentile(data, 90)}")
    print(f"P99:\t{np.percentile(data, 99)}")
    print(f"P99.9:\t{np.percentile(data, 99.9)}")

    
if __name__ == "__main__":
    file_path = 'data/figure_9/data.json'
    plot_figure(file_path)
    print('\nwork finished')


