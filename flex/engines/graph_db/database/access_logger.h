#ifndef GRAPHSCOPE_DATABASE_ACCESS_LOGGER_H_
#define GRAPHSCOPE_DATABASE_ACCESS_LOGGER_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <fstream>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <queue>
#include <string>
#include <thread>

#include "flex/storages/rt_mutable_graph/types.h"
#include "glog/logging.h"

namespace gs {

struct LogData{
    std::size_t address;
    std::size_t offset;
    timestamp_t timestamp;
};

class ThreadLog{
private:
    int thread_id;
    std::string filename;
    std::vector<LogData> log_data_list;
    int fd_;
    std::ofstream logFile;
    
public:

    ThreadLog():thread_id(-1),fd_(-1){}

    ~ThreadLog() {
        if (fd_!=-1) {
            // 写入剩余的时钟周期并关闭文件
            // for (const auto& log_data : log_data_list) {
            //     log_file << "Thread ID: " << std::setw(2) << std::setfill('0') << thread_id
            //             << " | Clock Cycles: " << cycle << std::endl;
            // }
            close();
        }
    }

    void open(int tid);
    void close();
    void log_append(std::size_t address, std::size_t offset, timestamp_t timestamp);
    void log_sync();
    void log_info(std::string info);
};
}
#endif