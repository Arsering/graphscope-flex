import matplotlib.pyplot as plt
import numpy as np
import re

# 第一组 mean 值
means1 = [
    1.75043e+07, 1.24472e+07, 1.2244e+07, 1.57009e+07, 1.36076e+07,
    2.3943e+07, 1.51812e+07, 1.49263e+07, 1.28216e+07, 1.25615e+07,
    1.51308e+07, 1.65766e+07, 1.49156e+07, 1.30292e+07
]

# 第二组 mean 值
means2 = [
    1.86142e+07, 1.49543e+07, 1.38991e+07, 1.7096e+07, 1.4569e+07,
    2.7517e+07, 1.58893e+07, 1.60607e+07, 1.64973e+07, 1.46771e+07,
    1.61974e+07, 1.73048e+07, 1.61152e+07, 1.50413e+07
]

def extract_second_mean_array_from_log(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()

    # 用于存储所有IC行的数据
    all_ic_blocks = []
    current_block = []

    for line in lines:
        if line.startswith("IC"):
            current_block.append(line)
        elif current_block:
            # 遇到非IC行，说明一个块结束
            if current_block:
                all_ic_blocks.append(current_block)
                current_block = []

    # 如果最后还有未加进来的块
    if current_block:
        all_ic_blocks.append(current_block)

    # 确保有两个IC块
    if len(all_ic_blocks) < 2:
        raise ValueError("未找到两个完整的 IC 数据块")

    second_block = all_ic_blocks[1]  # 取第二段
    means = []

    for line in second_block:
        match = re.search(r"mean:\s*([\d\.e\+]+)", line)
        if match:
            means.append(float(match.group(1)))

    return means


def plot_figure(means_list):
    # 标签名称：IC1 到 IC14
    labels = [f'IC{i}' for i in range(1, 15)]
    x = np.arange(len(labels))  # x轴位置
    width = 0.35  # 每组柱的宽度
    colors = ['skyblue', 'salmon']
    legends = ['MMAP 50%', 'GoCache 50%']
    # 创建图形
    plt.figure(figsize=(10, 6))
    for i, means in enumerate(means_list):
        plt.bar(x - width/2 + i*width, means, width, label=legends[i], color=colors[i])
    # plt.bar(x + width/2, means2, width, label='MMAP', color='salmon')

    # 图形美化
    plt.xlabel('IC Index')
    plt.ylabel('Mean Latency')
    plt.title('Comparison of Mean Latency (IC1 to IC14)')
    plt.xticks(x, labels, rotation=45)
    plt.legend()
    plt.tight_layout()

    # 显示图形
    plt.show()

    # 显示图像
    plt.savefig('figs/fig34.pdf', bbox_inches='tight')

if __name__ == '__main__':
    # MMAP 50%
    log_path_1 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-14-15:46:15/server/gs_log.log'
    # GoCache 50%
    log_path_2 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-14-15:24:47/server/gs_log.log'    
    # MMAP 30%
    log_path_3 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-14-16:32:08/server/gs_log.log'
    # GoCache 30%
    log_path_4 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-14-17:25:36/server/gs_log.log'
    # MMAP 20%
    log_path_5 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-14-19:42:19/server/gs_log.log'
    # GoCache 20%
    log_path_6 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-14-21:08:02/server/gs_log.log'
    means_1 = extract_second_mean_array_from_log(log_path_1)
    means_2 = extract_second_mean_array_from_log(log_path_2)
    means_3 = extract_second_mean_array_from_log(log_path_3)
    means_4 = extract_second_mean_array_from_log(log_path_4)
    means_5 = extract_second_mean_array_from_log(log_path_5)
    means_6 = extract_second_mean_array_from_log(log_path_6)
    print(means_1)
    print(means_2)
    print(means_3)
    print(means_4)
    print(means_5)
    print(means_6)
    plot_figure([means_5, means_6])