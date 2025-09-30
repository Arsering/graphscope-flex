import os
import glob
import re
import numpy as np

def get_thread_number(filename):
    # Extract number from filename using regex
    match = re.search(r'thread_log_(\d+)', filename)
    if match:
        return int(match.group(1))
    return -1

def traverse_thread_logs(log_path):    
    # Check if directory exists
    if not os.path.exists(log_path):
        print(f"Directory does not exist: {log_path}")
        return
    
    # Find all thread log files
    thread_logs = glob.glob(os.path.join(log_path, "*thread_log*"))
    
    # Filter logs with number > 29
    thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 29]
    
    # Dictionary to store arrays for each thread log
    thread_data = {}
    mean_latencies = np.zeros(21)
    num_queries = np.zeros(21)
    # Process each thread log file
    # print(f"Found {len(thread_logs)} thread log files with number > 29:")
    for log_file in thread_logs:
        filename = os.path.basename(log_file)
        thread_num = get_thread_number(filename)
        # print(f"\nProcessing: {filename} (Thread {thread_num})")
        
        # Initialize arrays for this thread log
        array1 = []
        array2 = []
        array3 = []
        
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    # Split the line into three elements
                    elements = line.strip().split()
                    if len(elements) == 3 and int(elements[2]) < 21:
                        array1.append(elements[0])
                        array2.append(elements[1])
                        array3.append(elements[2])
                        mean_latencies[int(elements[2])] += (int(elements[0])-int(elements[1]))
                        num_queries[int(elements[2])] += 1
            # Store arrays in dictionary
            thread_data[filename] = {
                'array1': array1,
                'array2': array2,
                'array3': array3
            }
            
            # # Print summary for this file
            # print(f"Processed {len(array1)} lines")
            # print(f"First few elements:")
            # print(f"Array 1: {array1[:5]}")
            # print(f"Array 2: {array2[:5]}")
            # print(f"Array 3: {array3[:5]}")
            
        except Exception as e:
            print(f"Error reading file {log_file}: {str(e)}")
    for i in range(15):
        mean_latencies[i] = mean_latencies[i] / num_queries[i]
    print(mean_latencies)
    return thread_data, mean_latencies

def traverse_thread_logs_1(log_path):    
    # Check if directory exists
    if not os.path.exists(log_path):
        print(f"Directory does not exist: {log_path}")
        return
    
    # Find all thread log files
    thread_logs = glob.glob(os.path.join(log_path, "*thread_log*"))
    
    # Filter logs with number > 29
    thread_logs = [log for log in thread_logs if get_thread_number(os.path.basename(log)) > 29]
    
    # Dictionary to store arrays for each thread log
    thread_data = {}
    mean_latencies = np.zeros(21)
    mean_latencies_GoCache_1 = np.zeros(21)
    mean_latencies_GoCache_2 = np.zeros(21)
    num_queries = np.zeros(21)
    # Process each thread log file
    # print(f"Found {len(thread_logs)} thread log files with number > 29:")
    for log_file in thread_logs:
        filename = os.path.basename(log_file)
        thread_num = get_thread_number(filename)
        # print(f"\nProcessing: {filename} (Thread {thread_num})")
        
        # Initialize arrays for this thread log
        array1 = []
        array2 = []
        array3 = []
        
        try:
            with open(log_file, 'r') as f:
                for line in f:
                    # Split the line into three elements
                    elements = line.strip().split()
                    if len(elements) == 5 and int(elements[4]) < 21:
                        array1.append(elements[0])
                        array2.append(elements[1])
                        array3.append(elements[3])
                        mean_latencies[int(elements[4])] += (int(elements[0])-int(elements[1]))
                        mean_latencies_GoCache_1[int(elements[4])] += int(elements[2])
                        mean_latencies_GoCache_2[int(elements[4])] += int(elements[3])
                        num_queries[int(elements[4])] += 1
            # Store arrays in dictionary
            thread_data[filename] = {
                'array1': array1,
                'array2': array2,
                'array3': array3
            }
            
            # # Print summary for this file
            # print(f"Processed {len(array1)} lines")
            # print(f"First few elements:")
            # print(f"Array 1: {array1[:5]}")
            # print(f"Array 2: {array2[:5]}")
            # print(f"Array 3: {array3[:5]}")
            
        except Exception as e:
            print(f"Error reading file {log_file}: {str(e)}")
    for i in range(15):
        mean_latencies[i] = mean_latencies[i] / num_queries[i]
        mean_latencies_GoCache_1[i] = mean_latencies_GoCache_1[i] / num_queries[i]
        mean_latencies_GoCache_2[i] = mean_latencies_GoCache_2[i] / num_queries[i]

    print(mean_latencies[1:])
    print(mean_latencies_GoCache_1[1:])
    print(mean_latencies_GoCache_2[1:])
    return thread_data, mean_latencies, mean_latencies_GoCache_1, mean_latencies_GoCache_2

if __name__ == "__main__":
    # MMAP 50%
    log_path_1 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-16-19:11:41/server/graphscope_logs'
    # GoCache 50%
    log_path_2 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-16-19:31:33/server/graphscope_logs'
    #MMAP 50%
    log_path_3 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-17-11:32:23/server/graphscope_logs'
    #BP 50%
    log_path_4 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-17-13:27:32/server/graphscope_logs'
    
    # thread_data_1, mean_latencies_1 = traverse_thread_logs(log_path_3)  # The path is hardcoded in the function
    # thread_data_2, mean_latencies_2 = traverse_thread_logs(log_path_4)  # The path is hardcoded in the function
    # print(mean_latencies_1-mean_latencies_2)
    
    log_path_1 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-19-20:34:24/server/graphscope_logs'
    log_path_2 = '/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-06-19-21:00:11/server/graphscope_logs'
    traverse_thread_logs_1(log_path_2)

