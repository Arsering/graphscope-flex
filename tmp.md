上周四Todo List
1. 整理目前buffer pool的数据结构以及运行逻辑，供讨论(已完成，需进一步完善)
2. 寻找pread (w/ or w/o) buffer pool比mmap好的workload
    1. 先跑通罗小简之前的pread实验 （只在LDBC上跑通，未在客户提供的worklaod上跑过）
3. 实验：每一个线程维护一个本地buffer pool，去掉并行带来overhead，确定buffer pool的性能上限（未做）
4. 看LDBC-SNB specification，确定不同query之间存在数据访问的局部性（连续的query访问的数据在空间上存在局部性）（已确定在LDBC的workload上存在）
5. 看天池比赛的代码，分析其cache的逻辑（其逻辑应该与KVell类似，即将数据partition，然后不同的partition由不同的后台线程管理）（已完成）

今日Todo List:
1. 继续完成上周四未完成的
2. 确定pread和mmap的性能差距（简单的读一个文件）
3. 理顺graphscope的IO逻辑，寻找优化空间