import os
import glob
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

LOG_NAME = 'sf30_30_order_unorder'

def analyze_thread_logs(log_dir):
    """
    分析指定目录下所有thread*.log文件中查询的时间分布情况
    
    Args:
        log_dir: 日志文件所在目录路径
    """
    OUTPUT = f'/data-1/yichengzhang/data/latest_gs_bp/zed-graphscope-flex/experiment_space/LDBC_SNB/python/result/query_latency/{LOG_NAME}'
    os.makedirs(OUTPUT, exist_ok=True)
    
    # 存储所有查询数据
    query_data = defaultdict(list)
    all_timestamps = []  # 存储所有查询的时间戳
    gt_21_timestamps = []  # 存储type>21的查询的时间戳
    
    # 准备输出文件
    output_file = os.path.join(OUTPUT, 'analysis_result.txt')
    with open(output_file, 'w') as f:
        # 获取所有thread*.log文件
        log_files = glob.glob(os.path.join(log_dir, "thread*.log"))
        
        for log_file in log_files:
            with open(log_file, 'r') as log_f:
                for line in log_f:
                    try:
                        # 解析每一行
                        # type_str, ts2_str, ts1_str = line.strip().split('|')
                        ts2_str, ts1_str, type_str = line.strip().split(' ')
                        query_type = int(type_str)
                        timestamp1 = float(ts1_str)
                        
                        all_timestamps.append(timestamp1)
                        if query_type > 21:
                            gt_21_timestamps.append(timestamp1)
                            query_data[query_type].append(timestamp1)
                    except (ValueError, IndexError):
                        continue
        
        if not gt_21_timestamps:
            f.write("没有找到type>21的查询\n")
            return
            
        # 计算基本统计信息
        total_queries = len(all_timestamps)
        gt_21_queries = len(gt_21_timestamps)
        ratio = gt_21_queries / total_queries if total_queries > 0 else 0
        
        f.write("\n总体统计信息:\n")
        f.write(f"总查询数: {total_queries}\n")
        f.write(f"type>21的查询数: {gt_21_queries}\n")
        f.write(f"type>21查询占比: {ratio:.2%}\n")
        
        # 分析时间分布
        gt_21_timestamps.sort()
        time_span = max(all_timestamps) - min(all_timestamps)
        num_intervals = 20  # 将时间范围分成20个区间
        interval_size = time_span / num_intervals
        
        # 计算每个时间区间内的查询数量
        intervals = defaultdict(int)
        for ts in gt_21_timestamps:
            interval_idx = int((ts - gt_21_timestamps[0]) / interval_size)
            intervals[interval_idx] += 1
        
        # 计算时间分布的均匀性
        queries_per_interval = [intervals[i] for i in range(num_intervals)]
        avg_queries = sum(queries_per_interval) / num_intervals
        variance = sum((count - avg_queries) ** 2 for count in queries_per_interval) / num_intervals
        std_dev = variance ** 0.5
        cv = std_dev / avg_queries if avg_queries > 0 else 0  # 变异系数

        # 将时间戳排序并分成100份
        sorted_timestamps = sorted(gt_21_timestamps)
        num_splits = 100
        split_size = len(sorted_timestamps) // num_splits
        split_timestamps = []
        
        # 获取每一份的起始时间
        for i in range(num_splits):
            split_idx = i * split_size
            split_timestamps.append(sorted_timestamps[split_idx])
            
        # 写入JSON文件
        import json
        with open(os.path.join(OUTPUT, 'update_time.json'), 'w') as json_file:
            json.dump(split_timestamps, json_file)
        
        f.write(f"\n时间分布分析:\n")
        f.write(f"总时间跨度: {time_span:.2f} 秒\n")
        f.write(f"每个时间区间大小: {interval_size:.2f} 秒\n")
        f.write(f"平均每个时间区间的查询数: {avg_queries:.2f}\n")
        f.write(f"标准差: {std_dev:.2f}\n")
        f.write(f"变异系数: {cv:.2f}\n")  # 变异系数越小，时间分布越均匀
        
        # 绘制时间分布图
        plt.figure(figsize=(15, 6))
        
        # 绘制直方图
        plt.subplot(1, 2, 1)
        plt.hist(gt_21_timestamps, bins=num_intervals, alpha=0.7)
        plt.title('Time Distribution of Queries (type>21)')
        plt.xlabel('Timestamp')
        plt.ylabel('Number of Queries')
        
        # 绘制累积分布图
        plt.subplot(1, 2, 2)
        plt.plot(gt_21_timestamps, range(len(gt_21_timestamps)))
        plt.title('Cumulative Query Distribution')
        plt.xlabel('Timestamp')
        plt.ylabel('Cumulative Number of Queries')
        
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT, 'time_distribution.png'))
        plt.close()
        # 按照查询类型排序
        sorted_query_data = dict(sorted(query_data.items()))
        query_data = sorted_query_data
        # 为每种查询类型创建时间分布图
        for query_type, timestamps in query_data.items():
            timestamps = pd.Series(timestamps)
            f.write(f"\n查询类型 {query_type} 的统计信息:\n")
            f.write(f"查询次数: {len(timestamps)}\n")
            f.write(f"时间跨度: {timestamps.max() - timestamps.min():.2f} 秒\n")
            
            plt.figure(figsize=(10, 6))
            plt.hist(timestamps, bins=min(50, len(timestamps)), alpha=0.7)
            plt.title(f'Query Type {query_type} Time Distribution')
            plt.xlabel('Timestamp')
            plt.ylabel('Frequency')
            plt.savefig(os.path.join(OUTPUT, f'query_type_{query_type}_time_distribution.png'))
            plt.close()

if __name__ == "__main__":
    log_dir = f"/data-1/yichengzhang/data/latest_gs_bp/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-02-16-22:24:20/server/graphscope_logs"
    analyze_thread_logs(log_dir)
