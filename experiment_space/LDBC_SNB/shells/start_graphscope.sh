#!/bin/bash
DISK_DEVICE=/dev/vdb
CUR_DIR=/mnt/nvme0n1/zyc/TriCache_Space/graphscope-flex

export SF=300

export Scale_Factor=sf${SF}
export INPUT_OUTPUT_DIR=${CUR_DIR}/experiment_space/LDBC_SNB
export DB_ROOT_DIR=/mnt/raid/${Scale_Factor}_db_BP
# export DB_ROOT_DIR=/mnt/nvme0n1/zyc/data/${Scale_Factor}_db_BP
# export DB_ROOT_DIR=/nvme0n1/Anew_db/${Scale_Factor}_db_BP

export QUERY_FILE=/mnt/nvme0n1/zyc/data/query_file/${Scale_Factor}
# export QUERY_FILE=/data-1/zhengyang/data/graphscope-flex/experiment_space/LDBC_SNB/logs/2025-04-02-19:28:16/server/graphscope_logs
# export QUERY_FILE=${INPUT_OUTPUT_DIR}/configurations/query.file

rm -rf ${DB_ROOT_DIR}/runtime/tmp/*
rm -rf ${DB_ROOT_DIR}/runtime/*
rm -rf ${DB_ROOT_DIR}/wal

export time=$(date "+%Y-%m-%d-%H:%M:%S")
export LOG_DIR=${INPUT_OUTPUT_DIR}/logs/${time}/server
mkdir -p ${LOG_DIR}/configurations
mkdir ${LOG_DIR}/graphscope_logs

# generate and save configuration file
# bash gen_bulk_load_yaml.sh
cp ${INPUT_OUTPUT_DIR}/configurations/graph_${SF}_bench.yaml ${LOG_DIR}/configurations/graph.yaml
cp ${INPUT_OUTPUT_DIR}/configurations/bulk_load_${SF}.yaml ${LOG_DIR}/configurations/bulk_load.yaml

# store shell file
mkdir ${LOG_DIR}/shells
cp -r ${INPUT_OUTPUT_DIR}/shells/$0 ${LOG_DIR}/shells/

# rm -rf ${DB_ROOT_DIR}/* && bulk_loader -B $[1024*1024*1024*90] -g ${LOG_DIR}/configurations/graph.yaml -l ${LOG_DIR}/configurations/bulk_load.yaml -p 30 -d ${DB_ROOT_DIR} &> ${LOG_DIR}/gs_log.log
# start iostat
# nohup iostat -d ${DISK_DEVICE} -t 1 > ${LOG_DIR}/iostat.log &

# export LD_LIBRARY_PATH=#LD_LIBRARY_PATH:/usr/local/lib
for thread_num in 40
do
    # expression="(1.28 + 0.0131 * $thread_num + 25) * 1024 * 1024 * 1024"
    # mem + 15
    # ro + 5.5
    # rw + 6.3
    # rw 6GB + 6.2
    # sf1000 150GB
    # sf1000 rw +17.5
    expression="1024*1024*1024*35.5"
    memory_capacity=$(python3 -c "print(int($expression))")
    echo ${memory_capacity} > /sys/fs/cgroup/memory/zyc_variable/memory.limit_in_bytes

    echo 1 > /proc/sys/vm/drop_caches
    echo 1 > /proc/sys/vm/drop_caches
    # numactl --cpunodebind=0 --membind=0 rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &> ${LOG_DIR}/gs_log.log&
    # numactl --cpunodebind=0 --membind=0 cgexec -g memory:zyc_variable rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &> ${LOG_DIR}/gs_log.log&
    # numactl --cpunodebind=0 --membind=0 cgexec -g memory:zyc_variable rt_bench_thread -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 300000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log&
    # memory_capacity=$(python3 -c "print(int(1024*1024*1024*80))")
    # numactl --cpunodebind=0 --membind=0 rt_bench_thread -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 300000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log
    # 15.2
    # numactl --cpunodebind=0 --membind=0 rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &> ${LOG_DIR}/gs_log.log &
    # 6 12 18 24 30 36
    # 5.85 
    # 27.2
    memory_capacity=$(python3 -c "print(int(1024*1024*1024*6))")
    # 15.2
    numactl --cpunodebind=0 --membind=0 rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &> ${LOG_DIR}/gs_log.log &
    # sleep 1000000s
    # bash run_client.sh

    # nohup rt_test1 -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${INPUT_OUTPUT_DIR}/configurations/graph_${SF}_bench.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 10000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log &
    
    # gdb --args 
    # cgexec -g memory:zyc_variable 
    # gdb --args 
    # export BATCH_SIZE_RATIO=0.
    # rt_bench_thread -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 1000000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log
    # gdb --args 
    # cgexec -g memory:zyc_variable rt_server -B ${memory_capacity} -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s ${thread_num} &> ${LOG_DIR}/gs_log.log &
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

# rm -rf ${DB_ROOT_DIR}/* && bulk_loader -B $[1024*1024*1024*100] -g ${LOG_DIR}/configurations/graph.yaml -l ${LOG_DIR}/configurations/bulk_load.yaml -p 64 -d ${DB_ROOT_DIR} &> ${LOG_DIR}/gs_log.log

# sleep 10s
# start top
# nohup top -p `cat ${LOG_DIR}/graphscope_logs/graphscope.pid` -b > ${LOG_DIR}/top.log  &
# sleep 600s
# timeout 300s perf record -F 999 -a -g -p `cat ${LOG_DIR}/graphscope_logs/graphscope.pid` -o ${LOG_DIR}/perf.data
# pkill -9 rt_bench_thread
# nohup perf record -F 999 -a -g -p `pidof rt_bench` -o ${LOG_DIR}/perf.data &