import matplotlib.pyplot as plt
import numpy as np
import re
import os



def plot_fig():
    # 示例数据
    categories = [f'IC-{i+1}' for i in range(14)]  # X轴标签
    values1 = np.random.randint(10, 50, size=14)   # 第一类数据
    values2 = np.random.randint(10, 50, size=14)   # 第二类数据

    means_0 = [1.93607e+06, 3.04455e+06, 1.51995e+07, 4.59119e+05, 1.70363e+08, 1.68308e+06,
            4.50901e+04, 1.45026e+06, 5.27356e+08, 8.07143e+06, 1.31433e+05, 1.28357e+07,
            1.46434e+05, 1.13016e+07]
    means_1 = [1.13105e+07, 2.3648e+07, 1.28046e+08, 4.07285e+06, 7.01345e+08, 1.20983e+07,
            1.57465e+05, 8.63733e+06, 3.65745e+09, 4.75098e+07, 6.80337e+05, 6.98472e+07,
            3.01053e+05, 6.81209e+07]
    means_2 = [4.58664e+07, 4.39564e+07, 1.43951e+08, 4.06036e+06, 7.25738e+08, 4.18955e+07,
            4.08853e+06, 4.60384e+07, 5.45016e+09, 5.21151e+07, 2.05624e+06, 1.0457e+08, 
            1.5381e+06, 5.37753e+08]

    value_1 = np.array(means_1)/np.array(means_0)
    value_2 = np.array(means_2)/np.array(means_0)

    x = np.arange(len(categories))  # X轴位置
    width = 0.35                    # 柱子的宽度

    fig, ax = plt.subplots(figsize=(10, 6))

    # 画柱状图
    rects1 = ax.bar(x - width/2, value_1, width, label='Type 1')
    rects2 = ax.bar(x + width/2, value_2, width, label='Type 2')



    # 添加标签和标题
    ax.set_ylabel('Values')
    ax.set_xlabel('Categories')
    ax.set_title('Two Types of Bars')
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=45)
    ax.legend()

    # # 添加数值标注
    # def add_labels(rects):
    #     for rect in rects:
    #         height = rect.get_height()
    #         ax.annotate(f'{height}',
    #                     xy=(rect.get_x() + rect.get_width() / 2, height),
    #                     xytext=(0, 3),  # 偏移
    #                     textcoords="offset points",
    #                     ha='center', va='bottom')

    # add_labels(rects1)
    # add_labels(rects2)

    plt.tight_layout()
    plt.savefig('figs/fig36-1.pdf', bbox_inches='tight')

def find_thread_logs(folder_path):
    """
    遍历文件夹，查找形如 thread_log_<数字>.log 的文件
    :param folder_path: 要遍历的文件夹路径
    :return: 符合条件的文件绝对路径列表
    """
    result = []
    pattern = re.compile(r"^thread_log_(\d+)\.log$")  # 匹配 thread_log_数字.log
    
    arr1, arr2, arr3 = [], [], []
    
    for filename in os.listdir(folder_path):
        match = pattern.match(filename)
        if match:
            num = int(match.group(1))
            if num > 30:
                file_path = os.path.join(folder_path, filename)

                with open(file_path, "r") as f:
                    for line in f:
                        parts = line.strip().split()
                        if len(parts) == 3:
                            a, b, c = map(int, parts)  # 这里用 float，改成 int 也可以
                            arr1.append(a)
                            arr2.append(b)
                            arr3.append(c)
    return arr1, arr2, arr3

def log_analysis(arr1, arr2, arr3):
    accesses = np.zeros(15)
    hits = np.zeros(15)
    for i in range(len(arr1)):
        if arr3[i] <= 14:
            accesses[arr3[i]] += arr1[i]
            hits[arr3[i]] += arr2[i]

    print(hits/accesses)
# 示例使用
if __name__ == "__main__":
    folder_50 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-08-13-14:31:35/server/graphscope_logs'
    folder_10 = "/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-08-07-23:43:06/server/graphscope_logs"  # 改成你的文件夹路径
    arr1, arr2, arr3 = find_thread_logs(folder_50)
    print(len(arr1))
    log_analysis(arr1, arr2, arr3)