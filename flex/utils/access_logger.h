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
#define DL false

void set_log_directory(const std::string& log_directory_i);
const std::string& get_log_directory();

__always_inline size_t GetSystemTime() {
  size_t hi, lo;
  __asm__ __volatile__("" : : : "memory");
  __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi));
  __asm__ __volatile__("" : : : "memory");
  return ((size_t) lo) | (((size_t) hi) << 32);
}

enum MmapArrayType {
  lf_index,
  column_num_or_date,
  column_string_view,
  column_string,
  nbr,
  adj_list
};
enum OperationType { read, write };

struct LogData {
  size_t address;
  int offset;
  MmapArrayType ma_type;
  OperationType o_type;
  size_t timestamp;

  LogData() { timestamp = 0; }

  size_t formalize(char* buf, size_t buf_capacity) const {
    size_t log_size = 0;

    log_size = snprintf(buf, buf_capacity, "\n%zu: [%d:%d] %zu:%d", timestamp,
                        ma_type, o_type, address, offset);
    if (log_size >= buf_capacity)
      return 0;
    return log_size;
  }

  bool empty() { return timestamp == 0; }

  static std::string get_format() {
    return "timestamp: [mmap_array_type:operation_type] offset:size ";
  }
};

class ThreadLog {
 private:
  int thread_id_;
  std::string filename_;
  size_t list_capacity_ = 1024;
  size_t list_size_ = 0;
  std::vector<LogData> log_data_list_;
  std::ofstream log_file_;

 public:
  ThreadLog() : thread_id_(-1) {}
  // ThreadLog(int tid) { open_log_file(tid); }
  ~ThreadLog() { close_log_file(); }

  void open_log_file(int tid) {
    thread_id_ = tid;
    filename_ = get_log_directory() + "/log_thread_" + std::to_string(tid);
    log_file_.open(filename_, std::ios::out);
    if (!log_file_.is_open()) {
      LOG(FATAL) << "Failed to open wal file";
    }
    std::string data = "log format: " + LogData::get_format();
    log_info(data);
    std::string mmap_array_type_content =
        "MmapArrayType { lf_index, column_num_or_date, column_string_view, "
        "column_string, "
        "nbr, adj_list }";
    std::string operation_type = "OperationType { read, write }";
    log_info(mmap_array_type_content);
    log_info(operation_type);
    log_file_.flush();

    list_capacity_ = 1024;
    log_data_list_.resize(list_capacity_);
    list_size_ = 0;
  }

  void close_log_file() {
    if (log_file_.is_open()) {
      log_file_.close();
    }
  }

  int log_append(std::size_t address, std::size_t offset,
                 MmapArrayType ma_type_i, OperationType o_type_i) {
    // log_info("append log");
    if (!log_file_.is_open()) {
      return -1;
    }
    log_data_list_[list_size_].address = address;
    log_data_list_[list_size_].offset = offset;
    log_data_list_[list_size_].ma_type = ma_type_i;
    log_data_list_[list_size_].o_type = o_type_i;
    log_data_list_[list_size_].timestamp = GetSystemTime();
    list_size_++;
    if (list_size_ == list_capacity_) {
      log_sync();
    }
    return 0;
  }

  int log_sync() {
    if (!log_file_.is_open()) {
      return -1;
    }
    size_t buf_capacity = 4096;
    char* buf = (char*) malloc(buf_capacity);
    size_t buf_size = 0;
    for (const auto& log_data : log_data_list_) {
      if (list_size_ == 0)
        break;
      list_size_--;
      size_t log_size =
          log_data.formalize(buf + buf_size, buf_capacity - buf_size);
      if (log_size == 0) {
        log_file_.write(buf, buf_size);
        buf_size = 0;
        log_size = log_data.formalize(buf + buf_size, buf_capacity - buf_size);
      }
      buf_size += log_size;
    }
    log_file_.write(buf, buf_size);
    free(buf);
    log_file_.flush();
    return 0;
  }

  int log_info(const std::string& info) {
    if (!log_file_.is_open()) {
      return -1;
    }
    log_sync();
    auto timestamp = GetSystemTime();
    std::string log_data = "\n" + std::to_string(timestamp) + ": " + info;
    log_file_.write(log_data.data(), log_data.size());
    log_file_.flush();

    // log_append(100, 101, MmapArrayType::adj_list, OperationType::read);
    return 0;
  }

  bool is_initalized() { return log_file_.is_open(); }

  int get_tid() { return thread_id_; }
};

bool thread_logger_is_empty();
void set_thread_logger(ThreadLog* access_logger);
ThreadLog* get_thread_logger();

}  // namespace gs
#endif