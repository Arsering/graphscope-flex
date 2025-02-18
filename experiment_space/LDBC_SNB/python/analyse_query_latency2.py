import os
import glob
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np

LOG_NAME = 'sf30_30_online_grouping_64_group_size'

def analyze_query_latency(log_dir):
    """
    分析指定目录下所有thread*.log文件中查询的延迟分布情况
    
    Args:
        log_dir: 日志文件所在目录路径
    """
    OUTPUT = f'/data-1/yichengzhang/data/latest_gs_bp/zed-graphscope-flex/experiment_space/LDBC_SNB/python/result/query_latency/{LOG_NAME}'
    os.makedirs(OUTPUT, exist_ok=True)
    
    # 存储所有查询数据，格式: {query_type: [(start_time, latency)]}
    query_data = defaultdict(list)
    
    # 准备输出文件
    output_file = os.path.join(OUTPUT, 'latency_analysis.txt')
    with open(output_file, 'w') as f:
        # 获取所有thread*.log文件
        log_files = glob.glob(os.path.join(log_dir, "thread*.log"))
        
        for log_file in log_files:
            with open(log_file, 'r') as log_f:
                for line in log_f:
                    try:
                        # 解析每一行
                        ts1_str, ts2_str, type_str = line.strip().split(' ')
                        query_type = int(type_str)
                        start_time = float(ts2_str)
                        end_time = float(ts1_str)
                        latency = (end_time - start_time)/3500/1000
                        
                        query_data[query_type].append((start_time, latency))
                    except (ValueError, IndexError):
                        continue
        
        if not query_data:
            f.write("没有找到任何查询记录\n")
            return
            
        # 按照查询类型排序
        sorted_query_data = dict(sorted(query_data.items()))
        query_data = sorted_query_data
        # 读取更新时间列表
        with open(f'/data-1/yichengzhang/data/latest_gs_bp/zed-graphscope-flex/experiment_space/LDBC_SNB/python/result/query_latency/{LOG_NAME}/update_time.json', 'r') as json_file:
            import json
            time_list = json.load(json_file)
        # time_list=time_list[10:]
        # 分析每种查询类型的延迟
        query_latency={}
        for query_type, data in query_data.items():
            start_times, latencies = zip(*data)
            # 将start_times和latencies转换为numpy数组以便处理
            start_times = np.array(start_times)
            latencies = np.array(latencies)
            
            # 初始化存储区间统计信息的列表
            interval_mean_latencies = []
            interval_p50_latencies = []
            interval_p99_latencies = []
            
            # 遍历时间区间
            for i in range(len(time_list)-1):
                start = time_list[i]
                end = time_list[i+1]
                
                # 找出该区间内的查询
                mask = (start_times >= start) & (start_times < end)
                interval_latencies = latencies[mask]
                
                if len(interval_latencies) > 0:
                    # 计算该区间的统计指标
                    interval_mean_latencies.append(np.mean(interval_latencies))
                    interval_p50_latencies.append(np.percentile(interval_latencies, 50))
                    interval_p99_latencies.append(np.percentile(interval_latencies, 99))
                else:
                    print(f"区间 {start} 到 {end} 内没有查询 {query_type}")
                    # 如果区间内没有查询，用0填充
                    interval_mean_latencies.append(0)
                    interval_p50_latencies.append(0)
                    interval_p99_latencies.append(0)
            
            query_latency[query_type]={
                'mean':interval_mean_latencies,
                'p50':interval_p50_latencies,
                'p99':interval_p99_latencies
            }
            # 绘制区间统计图
            plt.figure(figsize=(12, 6))
            x = range(len(interval_mean_latencies))
            plt.plot(x, interval_mean_latencies, label='Mean Latency', marker='o', markersize=2)
            plt.plot(x, interval_p50_latencies, label='P50 Latency', marker='s', markersize=2)
            plt.plot(x, interval_p99_latencies, label='P99 Latency', marker='^', markersize=2)
            plt.title(f'Query Type {query_type} Latency Statistics by Time Interval')
            plt.xlabel('Time Interval Index')
            plt.ylabel('Latency (ms)')
            plt.legend()
            plt.grid(True)
            plt.savefig(os.path.join(OUTPUT, f'query_type_{query_type}_interval_latency.png'))
            plt.close()
            
            # 将latencies转换为pandas Series以使用其统计功能
            latencies = pd.Series(latencies)
            
            f.write(f"\n查询类型 {query_type} 的延迟统计信息:\n")
            f.write(f"查询次数: {len(latencies)}\n")
            f.write(f"平均延迟: {latencies.mean():.6f} 秒\n")
            f.write(f"最小延迟: {latencies.min():.6f} 秒\n")
            f.write(f"最大延迟: {latencies.max():.6f} 秒\n")
            f.write(f"延迟标准差: {latencies.std():.6f} 秒\n")
            f.write(f"延迟中位数: {latencies.median():.6f} 秒\n")
        with open(os.path.join(OUTPUT, f'query_latency.json'), 'w') as f:
            json.dump(query_latency, f)

if __name__ == "__main__":
    log_dir = f"/data-1/yichengzhang/data/latest_gs_bp/zed-graphscope-flex/experiment_space/LDBC_SNB/logs/{LOG_NAME}/server/graphscope_logs"
    analyze_query_latency(log_dir)
