#!/bin/bash
# export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/boost-1.84/lib:/usr/local/boost_1_84_0/lib
export LD_LIBRARY_PATH=/usr/local/include/boost/lib:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/local/arrow-6.0.1/lib:$LD_LIBRARY_PATH
export CPLUS_INCLUDE_PATH=/usr/local/arrow-6.0.1/include:$CPLUS_INCLUDE_PATH

CUR_DIR=/mnt/nvme0n1/zhengyang/graphscope-flex

export SF=30
export BENCH_NAME=SNB # SNB or FinBench or linkbench
export Scale_Factor=sf${SF}
export CACHE_NAME=BP # MMAP or BP
export INPUT_OUTPUT_DIR=${CUR_DIR}/experiment_space/LDBC_SNB
# export DB_ROOT_DIR=/mnt/nvme3n1/zhengayang/LDBC_SNB_DB/${Scale_Factor}_db_BP
# export DB_ROOT_DIR=/mnt/nvme2n1/zhengyang/LDBC_SNB_DB/graphscope/${Scale_Factor}_db_BP
export DB_ROOT_DIR=/mnt/nvme0n1/zhengyang/LDBC_${BENCH_NAME}_DB/graphscope/${Scale_Factor}_db_${CACHE_NAME}

# export DB_ROOT_DIR=/nvme0n1/Anew_db/${Scale_Factor}_db_BP
# export DB_ROOT_DIR=/mnt/nvme0n1/zyc/data/${Scale_Factor}_db_BP

export QUERY_FILE=/mnt/nvme0n1/zyc/data/query_file/${Scale_Factor}
# sf30
# export QUERY_FILE=/mnt/nvme0n1/zhengyang/graphscope-flex/experiment_space/LDBC_SNB/logs/2026-01-07-23:38:53/server/graphscope_logs
# export QUERY_FILE=/mnt/nvme0n1/zhengyang/graphscope-flex/experiment_space/LDBC_SNB/logs/2026-01-15-16:27:01/server/graphscope_logs
# export QUERY_FILE=/mnt/nvme0n1/zhengyang/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-11-01-00:02:18/server/graphscope_logs
# export QUERY_FILE=/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-04-02-19:28:16/server/graphscope_logs
# export QUERY_FILE=${INPUT_OUTPUT_DIR}/configurations/query.file

sudo rm -rf ${DB_ROOT_DIR}/runtime/tmp/*
sudo rm -rf ${DB_ROOT_DIR}/runtime/*
sudo rm -rf ${DB_ROOT_DIR}/wal

export time=$(date "+%Y-%m-%d-%H:%M:%S")
export LOG_DIR=${INPUT_OUTPUT_DIR}/logs/${time}/server
mkdir -p ${LOG_DIR}/configurations
mkdir ${LOG_DIR}/graphscope_logs

# generate and save configuration file
# bash gen_bulk_load_yaml.sh
cp ${INPUT_OUTPUT_DIR}/configurations/${BENCH_NAME}_TriCache/${Scale_Factor}/graph.yaml ${LOG_DIR}/configurations/graph.yaml
cp ${INPUT_OUTPUT_DIR}/configurations/${BENCH_NAME}_TriCache/${Scale_Factor}/bulk_load.yaml ${LOG_DIR}/configurations/bulk_load.yaml

# store shell file
mkdir ${LOG_DIR}/shells
cp -r ${INPUT_OUTPUT_DIR}/shells/$0 ${LOG_DIR}/shells/

export LIVEGRAPH_NUM_CLIENTS=100
export OMP_NUM_THREADS=$LIVEGRAPH_NUM_CLIENTS
export OMP_PROC_BIND=true

export CACHE_PHY_SIZE=$(python3 -c "print(int(4 * 1024 * 1024 * 1024))")
export CACHE_VIRT_SIZE=$(expr 1024 \* 1024 \* 1024 \* 1024)
source /mnt/nvme0n1/zhengyang/TriCache_Space/TriCache/scripts/config.sh
export CACHE_CONFIG=$CACHE_16_SERVER_CONFIG
export CACHE_NUM_CLIENTS=$(expr $LIVEGRAPH_NUM_CLIENTS + 1)
export OMP_NUM_THREADS=32
export CACHE_MALLOC_THRESHOLD=$(expr 128 \* 1024)
export CACHE_ENABLE_SEGFAULT_HANDLER=1
export GRAPHSCOPE_NUM_CLIENTS=1

# rm -rf ${DB_ROOT_DIR}/* && bulk_loader -B $[1024*1024*1024*70] -g ${LOG_DIR}/configurations/graph.yaml -l ${LOG_DIR}/configurations/bulk_load.yaml -p 28 -d ${DB_ROOT_DIR} &> ${LOG_DIR}/gs_log.log
# start iostat
# nohup iostat -d ${DISK_DEVICE} -t 1 > ${LOG_DIR}/iostat.log &

# export LD_PRELOAD="/usr/local/lib/libtcmalloc.so"
# export HEAPPROFILE=${LOG_DIR}/heap_profile.log

# sf300 53.89GB
# sf1000 133.8GB
sudo pkill -9 rt_server
for thread_num in 5
do
    expression="(0 + 0.0131 * $thread_num + 6) * 1024 * 1024 * 1024"
    memory_capacity=$(python3 -c "print(int($expression))")
    # echo ${memory_capacity} > /sys/fs/cgroup/yz_variable/memory.limit_in_bytes

    echo 1 > /proc/sys/vm/drop_caches
    echo 1 > /proc/sys/vm/drop_caches
    memory_capacity=$(python3 -c "print(int(1024*1024*1024*2))")
    # nohup rt_test1 -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${INPUT_OUTPUT_DIR}/configurations/graph_${SF}_bench.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 10000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log &
    # cgexec -g memory:yz_variable 

    TZ=UTC  sudo -E LD_LIBRARY_PATH="$LD_LIBRARY_PATH" numactl -C !$CACHE_16_SERVER_CORES  \
        stdbuf -oL /usr/bin/time -v numactl --cpunodebind=0 --membind=0 rt_server -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${GRAPHSCOPE_NUM_CLIENTS} &> ${LOG_DIR}/gs_log.log
    
    # numactl --cpunodebind=0  --membind=0 rt_bench_thread -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 1000000 -b 100000 -r ${QUERY_FILE} &> ${LOG_DIR}/gs_log.log
    
    # systemd-run --scope -p MemoryMax=1.3G 
    # numactl --cpunodebind=0 --membind=0
    # gdb --args 
    # cgexec -g memory:yz_variable 
    # LD_PRELOAD=/usr/local/lib/libprofiler.so 
    # rt_bench_thread -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 2000000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log&
    # sudo valgrind --tool=massif \
    #              --unbuffered-fd=all \
    #              --fullpath-after= \
    #              --smc-check=all \
    #              --trace-syscalls=yes \
    #              rt_bench_thread -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 300000 -r ${QUERY_FILE} 
    # &>> ${LOG_DIR}/gs_log.log
    # gdb --args 
    # rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &> ${LOG_DIR}/gs_log.log &
    # nohup cgexec -g memory:zyc_variable rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &> ${LOG_DIR}/gs_log.log &
done

# cgexec -g memory:yz_variable 
# rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &>> ${LOG_DIR}/gs_log.log

# nohup rt_server -l ${LOG_DIR}/graphscope_logs -g ${INPUT_OUTPUT_DIR}/configurations/graph_${SF}_bench.yaml -d ${DB_ROOT_DIR} -s 50 &> ${LOG_DIR}/gs_log.log &

# rt_test -l ${LOG_DIR}/graphscope_logs -c /data/zhengyang/data/server_side/lgraph_db/sf${SF}_social_network -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s 1 > ${LOG_DIR}/gs_log.log 2>&1
# query_gen -c ${CUR_DIR}/lgraph_db/sf${SF}/social_network -o ${INPUT_OUTPUT_DIR}/configurations &>> ${LOG_DIR}/gs_log.log

# cgexec -g memory:yz_29.7g rt_bench_new -B $[1024*1024*1024*5] -l ${LOG_DIR}/graphscope_logs -g ${INPUT_OUTPUT_DIR}/configurations/graph_${SF}_bench.yaml -d ${DB_ROOT_DIR} -s 25 -w 0 -b 4000000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log
# -B $[1024*128]
# 9.348941802978516
# cgexec -g memory:yz_29.7g
# rt_bench -B $[1024*1024*1024*5] -l ${LOG_DIR}/graphscope_logs -g ${INPUT_OUTPUT_DIR}/configurations/graph_${SF}_bench.yaml -d ${DB_ROOT_DIR} -s 20 -w 0 -b 4000000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log

# rm -rf ${DB_ROOT_DIR}/* && bulk_loader -B $[1024*1024*1024*100] -g ${LOG_DIR}/configurations/graph.yaml -l ${LOG_DIR}/configurations/bulk_load.yaml -p ${thread_num} -d ${DB_ROOT_DIR} &> ${LOG_DIR}/gs_log.log

# sleep 10s
# start top
# nohup top -p `cat ${LOG_DIR}/graphscope_logs/graphscope.pid` -b > ${LOG_DIR}/top.log  &
# sleep 600s
# timeout 300s perf record -F 999 -a -g -p `cat ${LOG_DIR}/graphscope_logs/graphscope.pid` -o ${LOG_DIR}/perf.data
# pkill -9 rt_bench_thread
# nohup perf record -F 999 -a -g -p `pidof rt_bench` -o ${LOG_DIR}/perf.data &