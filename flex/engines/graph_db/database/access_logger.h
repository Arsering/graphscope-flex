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

    void open(int tid){
        filename="/data/zyc/data/access_log/log_thread_"+ std::to_string(tid);
        // fd_=::open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        // if (fd_ == -1) {
        //     LOG(FATAL) << "Failed to open wal file";
        // }else{
        //     std::string data="log is open";
        //     write(fd_, data.c_str(), data.length()+1);
        // }
        thread_id=tid;
        logFile.open(filename,std::ios::out);
        if (!logFile.is_open()) {
            LOG(FATAL) << "Failed to open wal file";
        }else{
            std::string data="log is open";
            //write(fd_, data.c_str(), data.length()+1);
            logFile<<data<<std::endl;
        }
    }
    void close(){
            // if (fd_ != -1) {
        //     ::close(fd_);
        //     fd_ = -1;
        // }
        if (logFile.is_open()) {
            log_sync();
            logFile.close();
        }
    }
    void log_append(std::size_t address, std::size_t offset, timestamp_t timestamp){
        log_info("append log !");
        LogData ldata;
        ldata.address=address;
        ldata.offset=offset;
        ldata.timestamp=timestamp;
        log_data_list.push_back(ldata);
        //if(log_data_list.size()>=64){
        log_sync();
        //}
    }
    void log_sync(){
        for (const auto& log_data : log_data_list) {
        logFile << log_data.address << " | "
                    << log_data.offset << " | "
                    << log_data.timestamp << "\n";
        }
    }
    void log_info(std::string info){
        logFile<<"thread_"<<thread_id<<": "<<info<<std::endl;
    }

    int get_tid(){
        return thread_id;
    }
};
}
#endif