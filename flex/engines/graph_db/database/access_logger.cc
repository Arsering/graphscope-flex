#include "flex/engines/graph_db/database/access_logger.h"
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ostream>
#include <string>
#include "access_logger.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

void ThreadLog::open(int tid){
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

void ThreadLog::close(){
    // if (fd_ != -1) {
    //     ::close(fd_);
    //     fd_ = -1;
    // }
    if (logFile.is_open()) {
        log_sync();
        logFile.close();
    }
}

void ThreadLog::log_append(std::size_t address, std::size_t offset, timestamp_t timestamp){
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

void ThreadLog::log_sync(){
    for (const auto& log_data : log_data_list) {
        logFile << log_data.address << " | "
                    << log_data.offset << " | "
                    << log_data.timestamp << "\n";
    }
}

void ThreadLog::log_info(std::string info){
    logFile<<"thread_"<<thread_id<<": "<<info<<std::endl;
}

}