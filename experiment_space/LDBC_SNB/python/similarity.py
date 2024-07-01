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
# sys.path.append(CUR_FILE_PATH + '/../')
# from plt_mine import set_mpl

           
def checkfile(file_name_1, file_name_2):
    f1 = open(file_name_1, 'rb+')
    data_1 = f1.read(os.path.getsize(file_name_1))
    
    f2 = open(file_name_2, 'rb+')
    data_2 = f2.read(os.path.getsize(file_name_2))

    single_datas_1 = data_1.split("eor#".encode())
    single_datas_2 = data_2.split("eor#".encode())
    single_datas_2_index = [0]*(int(len(single_datas_2)/2)+1)
    print(len(single_datas_1))  
    print(len(single_datas_2))

    for i in range(0, len(single_datas_2)-1, 2):
        # print(int(single_datas_2[i+1]))
        single_datas_2_index[int(single_datas_2[i+1])] = i
        
    for i in range(0, len(single_datas_1)-1, 2):
        # print(single_datas_1[i+1])
        if single_datas_1[i] != single_datas_2[single_datas_2_index[int(single_datas_1[i+1])]]:
            print("The two files have different content")
            print(single_datas_1[i+1])
            print(single_datas_1[i])
            print(len(single_datas_1[i]))
            print("=========")
            print(single_datas_2[single_datas_2_index[int(single_datas_1[i+1])]])
            print(len(single_datas_2[single_datas_2_index[int(single_datas_1[i+1])]]))
            return
    print("The two files have the same content")

    return

if __name__ == "__main__":
    # 主文件
    file_path_1 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-09-10:44:43/server/graphscope_logs/'
    # MMAP
    file_path_2 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-09-11:05:51/server/graphscope_logs/'
    # buffer pool
    file_path_3 = '/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-05-09-14:01:24/server/graphscope_logs/'
    
    file_name = "query_file.log"
    file_name = "result_file.log"
    checkfile(file_path_3+file_name, file_path_1+file_name)

    print('work finished\n')


