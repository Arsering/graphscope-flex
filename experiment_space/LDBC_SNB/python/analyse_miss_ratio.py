import os

log_path = "/mnt/nvme0n1/zyc/TriCache_Space/graphscope-flex/experiment_space/LDBC_SNB/logs/RO_50_GoCache_LRU/server/gs_log.log"

def analyze_miss_ratio():
    if not os.path.exists(log_path):
        print(f"日志文件 {log_path} 不存在")
        return
        
    with open(log_path, 'r') as f:
        # 读取所有行
        lines = f.readlines()
        # 获取最后1000行
        last_lines = lines[-1000:]
        
        total = 0
        count = 0
        
        for line in last_lines:
            if '=' in line:
                try:
                    # 提取=后面的数字
                    value = float(line.split('=')[1].strip())
                    total += value
                    count += 1
                except:
                    continue
                    
        if count > 0:
            avg = total / count
            print(f"最后1000行中=后数字的平均值为: {avg}")
        else:
            print("未找到有效数据")

if __name__ == "__main__":
    analyze_miss_ratio()
