#ifndef GRAPHSCOPE_DATABASE_ACCESS_LOGGER_H_
#define GRAPHSCOPE_DATABASE_ACCESS_LOGGER_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fstream>
#include <iostream>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <queue>
#include <string>
#include <thread>

#include "flex/storages/rt_mutable_graph/types.h"
#include "glog/logging.h"

namespace gs {

__always_inline size_t GetSystemTime() {
  size_t hi, lo;
  __asm__ __volatile__("" : : : "memory");
  __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi));
  __asm__ __volatile__("" : : : "memory");
  return ((size_t) lo) | (((size_t) hi) << 32);
}

enum MmapArrayType { lf_index, string_view, string, nbr, adj_list };
struct LogData {
  size_t address;
  size_t offset;
  MmapArrayType ma_type;
  size_t timestamp;

  LogData() { timestamp = 0; }

  size_t formalize(char* buf, size_t buf_capacity) const {
    size_t log_size = 0;

    log_size = snprintf(buf, buf_capacity, "\n%zu: [%d] %zu:%zu", timestamp,
                        ma_type, address, offset);
    if (log_size >= buf_capacity)
      return 0;
    return log_size;
  }
  bool empty() { return timestamp == 0; }
};

class ThreadLog {
 private:
  int thread_id_;
  std::string filename_;
  std::vector<LogData> log_data_list_;
  std::ofstream log_file_;

 public:
  ThreadLog() : thread_id_(-1) {}

  ~ThreadLog() { close_log_file(); }

  void open_log_file(int tid) {
    thread_id_ = tid;
    filename_ = "/data/zhengyang/data/tmp/log_thread_" + std::to_string(tid);
    log_file_.open(filename_, std::ios::out);
    if (!log_file_.is_open()) {
      LOG(FATAL) << "Failed to open wal file";
    } else {
      std::string data = "log is open";
      log_file_.write(data.c_str(), data.length() + 1);
    }
  }

  void close_log_file() {
    if (log_file_.is_open()) {
      log_file_.close();
    }
  }

  void log_append(std::size_t address, std::size_t offset) {
    log_info("append log !");
    LogData l_data;
    l_data.address = address;
    l_data.offset = offset;
    l_data.timestamp = GetSystemTime();
    log_data_list_.push_back(l_data);
    if (log_data_list_.size() >= 128) {
      log_sync();
    }
  }

  void log_sync() {
    size_t buf_capacity = 4096;
    char* buf = (char*) malloc(buf_capacity);
    size_t buf_size = 0;
    for (const auto& log_data : log_data_list_) {
      size_t log_size =
          log_data.formalize(buf + buf_size, buf_capacity - buf_size);
      if (log_size == 0) {
        log_file_.write(buf, buf_size);
        buf_size = 0;
        buf_size += log_data.formalize(buf + buf_size, buf_capacity - buf_size);
      }
    }
    log_file_.write(buf, buf_size);
    free(buf);
    log_data_list_.clear();
    log_file_.flush();
  }

  void log_info(std::string info) {
    log_file_ << GetSystemTime() << ": " << info;
  }

  bool is_initalized() { return log_file_.is_open(); }

  int get_tid() { return thread_id_; }
};

void set_thread_logger(ThreadLog* access_logger);
ThreadLog* get_thread_logger();

}  // namespace gs
#endif