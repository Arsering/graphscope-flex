import os
import glob
import numpy as np

def parse_log(file_path):
    """Parse a log file and return a list of (query_type, end_time, begin_time)."""
    data = []
    with open(file_path, 'r') as file:
        for line in file:
            if 'fail' in line:
                continue
            query_type, end_time, begin_time = line.strip().split('|')
            data.append((int(query_type), int(end_time), int(begin_time)))
    return data

def divide_updates(update_log, size):
    """Divide the update log into segments of specified size."""
    segments = []
    for i in range(0, len(update_log), size):
        segment = update_log[i:i + size]
        if segment:
            segments.append(segment)
    # for seg in segments:
    #     print(len(seg))
    return segments

def organize_reads_by_update_segments(update_segments, read_logs):
    """Organize read logs based on update segment end times."""
    all_results = []
    last_end=0
    for segment in update_segments:
        end_time = segment[-1][1]  # Use the last entry's end_time in this segment as the boundary
        
        # Prepare a dictionary to hold query times for this update segment
        segment_data = {}
        
        for read_log in read_logs:
            for query_type, read_end_time, read_begin_time in read_log:
                # Check if the read entry is within the current update segment's end_time boundary
                if read_end_time <= end_time and read_end_time>=last_end:
                    if query_type not in segment_data:
                        segment_data[query_type] = []
                    segment_data[query_type].append(read_end_time - read_begin_time)
        count=0
        for item in segment_data:
            count+=len(segment_data[item])
        print(count)
        # Append the result for this segment
        all_results.append(segment_data)
        last_end=end_time
    
    return all_results

# Specify the directory containing the log files
log_dir = "/data-1/yichengzhang/data/latest_gs_bp/graphscope-flex/experiment_space/LDBC_SNB/logs/2024-11-06-22:29:20/server/graphscope_logs"  # Replace 'dir' with the actual directory path

# Step 1: Parse update and read logs
update_log = parse_log(os.path.join(log_dir, "thread_log_0.log"))  # Update log file

# Step 2: Divide updates into segments of size `size`
size = 750000  # This should be set as desired
update_segments = divide_updates(update_log, size)
print(len(update_segments))
read_logs = [parse_log(f) for f in glob.glob(os.path.join(log_dir, "thread_log_*.log")) if "thread_log_0.log" not in f]  # Read log files
from collections import defaultdict
# Initialize a dictionary to store cumulative times and counts for each query type
query_sums = defaultdict(list)
# Step 3: Organize read logs by update segments and calculate query times
results = organize_reads_by_update_segments(update_segments, read_logs)

# for segment_data in results:
#     count=0
#     for query_type, times in segment_data.items():
#         if times:
#             # Calculate mean of current segment for each query type
#             count+=len(times)
#             query_sums[query_type].append(len(times))
#         else:
#             query_sums[query_type].append(0)
#     print(count)
# print(query_sums)
# print(result)
# Step 4: Calculate statistics for each query type in each segment
final_statistics = defaultdict(lambda: {'mean': [], 'p50': [], 'p90': [], 'p99': []})

for segment_data in results:
    for query_type, latencies in segment_data.items():
        if latencies:
            # Calculate required statistics
            mean_latency = np.mean(latencies)
            p50_latency = np.percentile(latencies, 50)
            p90_latency = np.percentile(latencies, 90)
            p99_latency = np.percentile(latencies, 99)
            
            # Store the statistics for this segment
            final_statistics[query_type]['mean'].append(mean_latency)
            final_statistics[query_type]['p50'].append(p50_latency)
            final_statistics[query_type]['p90'].append(p90_latency)
            final_statistics[query_type]['p99'].append(p99_latency)
        else:
            # If there are no latencies, append 0 to maintain segment count consistency
            final_statistics[query_type]['mean'].append(0)
            final_statistics[query_type]['p50'].append(0)
            final_statistics[query_type]['p90'].append(0)
            final_statistics[query_type]['p99'].append(0)

# Output the final dictionary with statistics for each query type
print(dict(final_statistics))