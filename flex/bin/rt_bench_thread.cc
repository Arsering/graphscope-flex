/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "grape/util.h"

#include <unistd.h>
#include <boost/fiber/all.hpp>
#include <boost/program_options.hpp>
#include <chrono>
#include <fstream>
// #include <hiactor/core/actor-app.hh>
#include <iostream>
#include <thread>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
// #include "flex/engines/http_server/executor_group.actg.h"
// #include "flex/engines/http_server/generated/executor_ref.act.autogen.h"
// #include "flex/engines/http_server/graph_db_service.h"

#include <glog/logging.h>

namespace bpo = boost::program_options;
// using namespace std::chrono_literals;
class Req {
 public:
  static Req& get() {
    static Req r;
    return r;
  }
  void init(size_t warmup_num, size_t benchmark_num) {
    warmup_num_ = warmup_num;
    num_of_reqs_ = warmup_num + benchmark_num;
    num_of_reqs_unique_ = reqs_.size();
    // num_of_reqs_unique_ = 2000000;
    start_.resize(num_of_reqs_);
    end_.resize(num_of_reqs_);
    cur_ = 0;

    std::cout << "warmup count: " << warmup_num_
              << "; benchmark count: " << num_of_reqs_ << "\n";
    if (!log_thread_.joinable())
      log_thread_ = std::thread([this]() { logger(); });
  }

  void load(const std::string& file_path) {
    LOG(INFO) << "load queries from " << file_path + "query_file.log" << "\n";
    std::ifstream query_file;
    query_file.open(file_path + "query_file.log", std::ios::in);

    const size_t size = 4096;
    std::vector<char> buffer(size);
    std::vector<char> tmp(size);
    size_t index = 0;
    size_t log_count = 0;
    bool mark = true;
    size_t req_id_cur = 0;

    while (true) {
      query_file.read(buffer.data(), size);
      auto len = query_file.gcount();
      if (len == 0)
        break;
      for (size_t i = 0; i < len; ++i) {
        tmp[index++] = buffer[i];
        if (index >= 4 && tmp[index - 1] == '#') {
          if (tmp[index - 4] == 'e' && tmp[index - 3] == 'o' &&
              tmp[index - 2] == 'r') {
            if (mark) {
              reqs_.emplace_back(
                  std::string(tmp.begin(), tmp.begin() + index - 4));
              mark = false;
            } else {
              auto req_id = std::stoull(
                  std::string(tmp.begin(), tmp.begin() + index - 4));
              if (req_id != req_id_cur)
                continue;
              req_ids_.emplace_back(req_id);
              mark = true;
              req_id_cur++;
            }
            index = 0;
          }
        }
      }

      buffer.clear();
    }
    LOG(INFO) << "Number of query = " << reqs_.size();
    query_file.close();
    num_of_reqs_ = reqs_.size();
  }

  void load_result(const std::string& file_path) {
    LOG(INFO) << "load results from " << file_path + "result_file.log";
    std::ifstream fi;
    fi.open(file_path + "result_file.log", std::ios::in);

    const size_t size = 4096;
    std::vector<char> buffer(size);
    std::vector<char> tmp(size * 128);
    size_t index = 0;
    bool mark = false;
    size_t req_id_cur = 0, req_id;
    size_t last_index = 0;

    while (true) {
      fi.read(buffer.data(), size);
      auto len = fi.gcount();
      if (len == 0)
        break;
      for (size_t i = 0; i < len; ++i) {
        tmp[index++] = buffer[i];
        if (index >= 4 && tmp[index - 1] == '#') {
          if (tmp[index - 4] == 'e' && tmp[index - 3] == 'o' &&
              tmp[index - 2] == 'r') {
            try {
              auto str_tmp = std::string(tmp.begin() + last_index,
                                         tmp.begin() + index - 4);
              req_id = std::stoull(str_tmp);
            } catch (const std::exception& e) {
              last_index = index - 1 + 1;
              continue;
            }
            if (req_id != req_id_cur) {
              last_index = index - 1 + 1;
              continue;
            }

            results_.emplace_back(
                std::string(tmp.begin(), tmp.begin() + last_index - 4));
            req_id_cur++;
            index = 0;
          }
        }
      }

      buffer.clear();
    }

    fi.close();
    LOG(INFO) << "load result file successfully";
    gbp::get_results_vec() = results_;
  }

  void do_query(size_t thread_id) {
    while (true) {
      auto id = cur_.fetch_add(1);
      if (id >= num_of_reqs_) {
        return;
      }

      start_[id] = gbp::GetSystemTime();
      gbp::get_query_id().store(req_ids_[id % num_of_reqs_unique_]);

      auto ret = gs::GraphDB::get().GetSession(thread_id).Eval(
          reqs_[id % num_of_reqs_unique_]);
      end_[id] = gbp::GetSystemTime();
    }
    return;
  }

  bool simulate(size_t thread_num = 10) {
    std::vector<std::thread> workers;
    assert(cur_ == 0);
    for (size_t i = 0; i < thread_num; i++) {
      workers.emplace_back([this, i]() { do_query(i); });
    }

    for (auto& worker : workers) {
      worker.join();
    }
    return true;
  }

  void output() {
    // std::ofstream profiling_file(log_data_path + "/profiling.log",
    //                              std::ios::out);
    // profiling_file << "LOG Format: Query Type | latency (OV)" << std::endl;

    std::vector<long long> vec(29, 0);
    std::vector<int> count(29, 0);
    std::vector<std::vector<long long>> ts(29);
    for (size_t idx = 0; idx < num_of_reqs_; idx++) {
      auto& s = reqs_[idx % num_of_reqs_unique_];
      size_t id = static_cast<size_t>(s.back()) - 1;
      // auto tmp = std::chrono::duration_cast<std::chrono::microseconds>(
      //                end_[idx] - start_[idx])
      //                .count();
      auto tmp = end_[idx] - start_[idx];
      // profiling_file << id << " | " << tmp << std::endl;
      ts[id].emplace_back(tmp);
      vec[id] += tmp;
      count[id] += 1;
    }

    std::vector<std::string> queries = {
        "IC1", "IC2",  "IC3",  "IC4",  "IC5",  "IC6",  "IC7", "IC8",
        "IC9", "IC10", "IC11", "IC12", "IC13", "IC14", "IS1", "IS2",
        "IS3", "IS4",  "IS5",  "IS6",  "IS7",  "IU1",  "IU2", "IU3",
        "IU4", "IU5",  "IU6",  "IU7",  "IU8"};
    for (auto i = 0; i < vec.size(); ++i) {
      size_t sz = ts[i].size();
      if (sz > 0) {
        std::cout << queries[i] << "; mean: " << vec[i] * 1.0 / count[i]
                  << "; counts: " << count[i] << "; ";

        std::sort(ts[i].begin(), ts[i].end());
        std::cout << " min: " << ts[i][0] << "; ";
        std::cout << " max: " << ts[i].back() << "; ";
        std::cout << " P50: " << ts[i][sz / 2] << "; ";
        std::cout << " P90: " << ts[i][sz * 9 / 10] << "; ";
        std::cout << " P95: " << ts[i][sz * 95 / 100] << "; ";
        std::cout << " P99: " << ts[i][sz * 99 / 100] << "\n";
      }
    }
    std::cout << "unit: MICROSECONDS\n";
  }
  void LoggerStop() { logger_stop_ = true; }

 private:
  Req() : cur_(0), warmup_num_(0) {}
  ~Req() {
    logger_stop_ = true;
    log_thread_.join();
  }
  void logger() {
    size_t operation_count_pre = 0;
    size_t operation_count_now = 0;
    size_t read_count_pre = 0;
    size_t read_count_now = 0;
    size_t fetch_count_pre = 0;
    size_t fetch_count_now = 0;
    size_t read_count_per_query_pre = 0;
    size_t read_count_per_query_now = 0;

    while (true) {
      sleep(1);
      operation_count_now = gbp::get_counter_query().load();
      // read_count_now = gbp::debug::get_counter_read().load();
      // fetch_count_now = gbp::debug::get_counter_fetch().load();
      // read_count_per_query_now =
      // gbp::debug::get_counter_fetch_unique().load();
      LOG(INFO) << "Throughput (Total) [" << operation_count_now / 10000 << "w"
                << operation_count_now % 10000 << "]" << "(last 1s) ["
                << (operation_count_now - operation_count_pre) / 10000 << "w"
                << (operation_count_now - operation_count_pre) % 10000 << "]"
                << " | Read Count (Total) [" << read_count_now / 10000
                << "w] (last 1s) [" << (read_count_now - read_count_pre) / 10000
                << "w" << (read_count_now - read_count_pre) % 10000 << "]"
                << " | Fetch Count (Total) [" << fetch_count_now / 10000
                << "w] (last 1s) ["
                << (fetch_count_now - fetch_count_pre) / 10000 << "w"
                << (fetch_count_now - fetch_count_pre) % 10000 << "]"
                << " | Fetch Count unique (Total) ["
                << read_count_per_query_now / 10000 << "w] (last 1s) ["
                << (read_count_per_query_now - read_count_per_query_pre) / 10000
                << "w"
                << (read_count_per_query_now - read_count_per_query_pre) % 10000
                << "]" << " | Num of free page in BP = "
#if !OV
                << gbp::BufferPoolManager::GetGlobalInstance().GetFreePageNum()
#endif
          ;
      operation_count_pre = operation_count_now;
      read_count_pre = read_count_now;
      fetch_count_pre = fetch_count_now;
      read_count_per_query_pre = read_count_per_query_now;
      if (logger_stop_)
        break;
    }
  }

  std::atomic<size_t> cur_;
  size_t warmup_num_;
  size_t num_of_reqs_;
  size_t num_of_reqs_unique_;
  std::vector<std::string> reqs_;
  std::vector<size_t> req_ids_;
  std::vector<std::string> results_;
  // std::vector<std::chrono::system_clock::time_point> start_;
  // std::vector<std::chrono::system_clock::time_point> end_;
  std::vector<size_t> start_;
  std::vector<size_t> end_;

  std::thread log_thread_;
  std::atomic<bool> logger_stop_ = false;
  // std::vector<executor_ref> executor_refs_;
};

int main(int argc, char** argv) {
  gbp::warmup_mark().store(0);

  size_t pool_size_Byte = 1024LU * 1024LU * 1024LU * 10;
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("shard-num,s",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "shard number of actor system")(
      "http-port,p", bpo::value<uint16_t>()->default_value(10000),
      "http port of query handler")("graph-config,g", bpo::value<std::string>(),
                                    "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "warmup-num,w", bpo::value<uint32_t>()->default_value(0),
      "num of warmup reqs")("benchmark-num,b",
                            bpo::value<uint32_t>()->default_value(0),
                            "num of benchmark reqs")(
      "req-file,r", bpo::value<std::string>(), "requests file")(
      "log-data-path,l", bpo::value<std::string>(), "log data directory path")(
      "buffer-pool-size,B",
      bpo::value<uint64_t>()->default_value(pool_size_Byte),
      "size of buffer pool");

  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = true;

  bpo::variables_map vm;
  bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
  bpo::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }
  if (vm.count("version")) {
    std::cout << "GraphScope/Flex version " << FLEX_VERSION << std::endl;
    return 0;
  }

  bool enable_dpdk = false;
  uint32_t shard_num = vm["shard-num"].as<uint32_t>();

  std::string graph_schema_path = "";
  std::string data_path = "";
  std::string bulk_load_config_path = "";
  std::string log_data_path = "";

  if (!vm.count("graph-config")) {
    LOG(ERROR) << "graph-config is required";
    return -1;
  }
  graph_schema_path = vm["graph-config"].as<std::string>();
  if (!vm.count("data-path")) {
    LOG(ERROR) << "data-path is required";
    return -1;
  }
  data_path = vm["data-path"].as<std::string>();
  if (!vm.count("log-data-path")) {
    LOG(ERROR) << "log-data-path is required";
    return -1;
  }
  log_data_path = vm["log-data-path"].as<std::string>();

  std::ofstream pid_file(log_data_path + "/graphscope.pid", std::ios::out);
  LOG(INFO) << log_data_path + "/graphscope.pid";
  pid_file << getpid();
  pid_file.flush();
  pid_file.close();
  gbp::get_query_file(log_data_path);
  gbp::get_result_file(log_data_path);
  LOG(INFO) << "Launch Performance Logger";
  gbp::PerformanceLogServer::GetPerformanceLogger().Start(
      log_data_path + "/performance.log", "nvme0n1");
  gbp::get_log_dir() = log_data_path;
  gbp::get_db_dir() = data_path;

  gbp::log_enable().store(false);
  setenv("TZ", "Asia/Shanghai", 1);
  tzset();
#if OV
  gbp::warmup_mark().store(0);
#else
  size_t pool_num = 10;
  gbp::warmup_mark().store(0);

  if (vm.count("buffer-pool-size")) {
    pool_size_Byte = vm["buffer-pool-size"].as<uint64_t>();
  }
  LOG(INFO) << "pool_size_Byte = " << pool_size_Byte << " Bytes";
  gbp::BufferPoolManager::GetGlobalInstance().init(
      pool_num, CEIL(pool_size_Byte, gbp::PAGE_SIZE_MEMORY) / pool_num,
      pool_num);

#ifdef DEBUG
  gbp::BufferPoolManager::GetGlobalInstance().ReinitBitMap();
#endif
#endif

  sleep(10);

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  LOG(INFO) << "Start loading graph";
  db.Init(schema, data_path, shard_num);

  t0 += grape::GetCurrentTime();
  uint32_t warmup_num = vm["warmup-num"].as<uint32_t>();
  uint32_t benchmark_num = vm["benchmark-num"].as<uint32_t>();
  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

#if !OV
  t0 = -grape::GetCurrentTime();
  gbp::warmup_mark().store(0);
  LOG(INFO) << "Warmup start";
  // gbp::BufferPoolManager::GetGlobalInstance().WarmUp();
  LOG(INFO) << "Warmup finish";
  gbp::warmup_mark().store(1);
  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Finished BufferPool warm up, elapsed " << t0 << " s";

  LOG(INFO) << "Clean start";
  // gbp::BufferPoolManager::GetGlobalInstance().Clean();
  LOG(INFO) << "Clean finish";
#else
  gbp::warmup_mark().store(1);
  LOG(INFO) << "Clean start";
  // gbp::CleanMAS();
  LOG(INFO) << "Clean finish";
#endif

  std::string req_file = vm["req-file"].as<std::string>();
  Req::get().load(req_file);
  // Req::get().load_result(req_file);
  for (size_t idx = 0; idx < 2; idx++) {
    Req::get().init(warmup_num, benchmark_num);

    // hiactor::actor_app app;
    gbp::log_enable().store(true);
    sleep(10);
    size_t ssd_io_byte = std::get<0>(gbp::SSD_io_bytes());
    auto cpu_cost_before = gbp::GetCPUTime();

    auto begin = std::chrono::system_clock::now();
    gbp::get_counter_global(10).store(0);
    gbp::get_counter_global(11).store(0);

    Req::get().simulate(shard_num);
    auto end = std::chrono::system_clock::now();
    auto cpu_cost_after = gbp::GetCPUTime();

    ssd_io_byte = std::get<0>(gbp::SSD_io_bytes()) - ssd_io_byte;

    LOG(INFO) << "CPU Cost = "
              << (std::get<0>(cpu_cost_after) - std::get<0>(cpu_cost_before)) /
                     1000000.0
              << "u/"
              << (std::get<1>(cpu_cost_after) - std::get<1>(cpu_cost_before)) /
                     1000000.0
              << "s (second)";
    LOG(INFO) << "SSD IO = " << ssd_io_byte << "(Byte)";
    LOG(INFO) << "Host IO = " << gbp::get_counter_global(10).load() << "(Byte)";
    LOG(INFO) << "Memory IO = "
              << gbp::get_counter_global(11).load() * gbp::PAGE_SIZE_MEMORY
              << "(Byte)";

    gbp::warmup_mark().store(0);

    std::cout << "cost time:"
              << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       begin)
                     .count()
              << "\n";
    Req::get().output();
  }

  Req::get().LoggerStop();

  auto memory_usages =
      gbp::BufferPoolManager::GetGlobalInstance().GetMemoryUsage();

  LOG(INFO) << std::get<0>(memory_usages) << " | " << std::get<1>(memory_usages)
            << " | " << std::get<2>(memory_usages) << " | "
            << std::get<3>(memory_usages) << " | "
            << std::get<4>(memory_usages);
}