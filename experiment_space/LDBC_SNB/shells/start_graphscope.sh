#!/bin/bash
DISK_DEVICE=/dev/vdb
CUR_DIR=/data/zhengyang/data/graphscope-flex

export SF=30

export Scale_Factor=sf${SF}
export INPUT_OUTPUT_DIR=${CUR_DIR}/experiment_space/LDBC_SNB
export DB_ROOT_DIR=/nvme0n1/lgraph_db/${Scale_Factor}_db_t
# export DB_ROOT_DIR=${INPUT_OUTPUT_DIR}/lgraph_db/${Scale_Factor}_db
export QUERY_FILE=/data/zhengyang/data/offline/
# export QUERY_FILE=${INPUT_OUTPUT_DIR}/configurations/query.file

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

# start iostat
# nohup iostat -d ${DISK_DEVICE} -t 1 > ${LOG_DIR}/iostat.log &

export LD_LIBRARY_PATH=#LD_LIBRARY_PATH:/usr/local/lib
for thread_num in 30
do
    expression="(0.6967 + 0.0131 * $thread_num + 25) * 1024 * 1024 * 1024"
    memory_capacity=$(python3 -c "print(int($expression+5))")
    echo ${memory_capacity} > /sys/fs/cgroup/memory/yz_variable/memory.limit_in_bytes

    echo 1 > /proc/sys/vm/drop_caches
    nohup rt_bench_thread -B $[1024*1024*1024*5] -l ${LOG_DIR}/graphscope_logs -g ${INPUT_OUTPUT_DIR}/configurations/graph_${SF}_bench.yaml -d ${DB_ROOT_DIR} -s ${thread_num} -w 0 -b 2000000 -r ${QUERY_FILE} &>> ${LOG_DIR}/gs_log.log &
done
# cgexec -g memory:yz_variable 
# nohup rt_server -B $[1024*1024*1024*100] -l ${LOG_DIR}/graphscope_logs -g ${LOG_DIR}/configurations/graph.yaml -d ${DB_ROOT_DIR} -s 30 &> ${LOG_DIR}/gs_log.log &

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
# # nohup perf record -F 999 -a -g -p `pidof rt_bench` -o ${LOG_DIR}/perf.data &
