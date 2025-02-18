import os

dir1='sf30_30_online_grouping_large_group_size'
dir2='sf30_30_update_append_only'

root_dir='/data-1/yichengzhang/data/latest_gs_bp/zed-graphscope-flex/experiment_space/LDBC_SNB/python/result/query_latency'

data_dir1=os.path.join(root_dir, dir1)
data_dir2=os.path.join(root_dir, dir2)

import json
import matplotlib.pyplot as plt
import os

# 创建保存结果的目录
output_dir = '/data-1/yichengzhang/data/latest_gs_bp/zed-graphscope-flex/experiment_space/LDBC_SNB/python/result/compare_result'
os.makedirs(output_dir, exist_ok=True)

    # 复制文件到output_dir
import shutil
# 读取两个json文件
with open(os.path.join(data_dir1, 'query_latency.json'), 'r') as f:

    shutil.copy(os.path.join(data_dir1, 'query_latency.json'), os.path.join(output_dir, 'large_group_size_query_latency.json'))
    data1 = json.load(f)
with open(os.path.join(data_dir2, 'query_latency.json'), 'r') as f:
    shutil.copy(os.path.join(data_dir2, 'query_latency.json'), os.path.join(output_dir, 'append_only_query_latency.json'))
    data2 = json.load(f)



# 获取并排序keys
keys1 = sorted([int(k) for k in data1.keys()])
keys2 = sorted([int(k) for k in data2.keys()])
# 准备数据
metrics = ['mean', 'p50', 'p99']

# 创建两个文件来保存平均值
avg_file1 = os.path.join(output_dir, 'online_grouping_64_averages.txt')
avg_file2 = os.path.join(output_dir, 'append_only_averages.txt')

with open(avg_file1, 'w') as f1, open(avg_file2, 'w') as f2:
    for key in keys1:
        f1.write(f"Query {key}:\n")
        f2.write(f"Query {key}:\n")
        for metric in metrics:
            # 计算平均值
            avg1 = sum(data1[str(key)][metric]) / len(data1[str(key)][metric])
            avg2 = sum(data2[str(key)][metric]) / len(data2[str(key)][metric])
            
            # 写入文件
            f1.write(f"{metric}: {avg1:.4f}\n")
            f2.write(f"{metric}: {avg2:.4f}\n")
            
            # 为每个metric创建对应的文件夹
            metric_dir = os.path.join(output_dir, metric)
            os.makedirs(metric_dir, exist_ok=True)
            
            # 绘制图表
            plt.figure(figsize=(10, 6))
            plt.plot(data1[str(key)][metric], label=f'online grouping 64')
            plt.plot(data2[str(key)][metric], label=f'append only')
            plt.title(f'Query {key} - {metric}')
            plt.xlabel('Time')
            plt.ylabel('Latency (ms)')
            plt.legend()
            plt.grid(True)
            plt.ylim(bottom=0)  # 设置y轴从0开始
            plt.savefig(os.path.join(metric_dir, f'query_{key}_{metric}.pdf'))
            plt.close()
        
        f1.write("\n")
        f2.write("\n")


