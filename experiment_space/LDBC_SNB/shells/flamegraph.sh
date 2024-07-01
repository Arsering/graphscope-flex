set -x

export FLAME_GRAPH_DIR=/data/zhengyang/data/FlameGraph
export DIR=/data/zhengyang/data/experiment_space/LDBC_SNB/logs/2024-06-26-18:51:09/server

cd ${DIR}
perf script -i perf.data > out.perf
${FLAME_GRAPH_DIR}/stackcollapse-perf.pl out.perf > out.folded
${FLAME_GRAPH_DIR}/flamegraph.pl out.folded > perf.svg