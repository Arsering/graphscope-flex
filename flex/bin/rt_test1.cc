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
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

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

    LOG(INFO) << "warmup count: " << warmup_num_
              << "; benchmark count: " << num_of_reqs_ << "\n";

    // run_time_req_ids_.resize(num_of_reqs_);
    // std::iota(run_time_req_ids_.begin(), run_time_req_ids_.end(), 0);

    if (!log_thread_.joinable())
      log_thread_ = std::thread([this]() { logger(); });

    gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
    auto person_label_id = graph.schema().get_vertex_label_id("PERSON");
    auto vertex_num = graph.vertex_num(person_label_id);
    oids_.resize(vertex_num);
    for (auto idx = 0; idx < vertex_num; idx++) {
      oids_[idx] = graph.get_oid(person_label_id, idx);
    }
    // std::random_device rd;
    std::mt19937 rng(323556435);  // 此处每次生成的随机数序列是一样的
    std::shuffle(oids_.begin(), oids_.end(), rng);
  }

  void load_result(const std::string& file_path) {
    {
      LOG(INFO) << "load results from " << file_path + "/result_file_string.log"
                << "\n";
      FILE* result_file_string =
          ::fopen((file_path + "/result_file_string.log").c_str(), "r");
      FILE* result_file_string_view =
          ::fopen((file_path + "/result_file_string_view.log").c_str(), "r");
      assert(result_file_string != nullptr);
      assert(result_file_string_view != nullptr);

      size_t size = 4096 * 64;
      std::vector<char> buffer(size);

      size_t length = 0;
      while (true) {
        auto ret = ::fread(&length, sizeof(size_t), 1, result_file_string_view);
        if (ret == 0)
          break;
        assert(length < size);

        if (length == 0) {
          gbp::get_results_vec().emplace_back(std::string());
          continue;
        }
        ::fread(buffer.data(), length, 1, result_file_string);
        gbp::get_results_vec().emplace_back(
            std::string(buffer.data(), buffer.data() + length));
      }
    }
    LOG(INFO) << "Number of results = " << gbp::get_results_vec().size();
  }

  void load_query(const std::string& file_path) {
    LOG(INFO) << "load queries from " << file_path + "/query_file_string.log"
              << "\n";
    FILE* query_file_string =
        ::fopen((file_path + "/query_file_string.log").c_str(), "r");
    FILE* query_file_string_view =
        ::fopen((file_path + "/query_file_string_view.log").c_str(), "r");
    assert(query_file_string != nullptr);
    assert(query_file_string_view != nullptr);

    const size_t size = 4096;
    std::vector<char> buffer(size);
    size_t length = 0;

    while (true) {
      auto ret = ::fread(&length, sizeof(size_t), 1, query_file_string_view);
      if (ret == 0)
        break;

      if (length == 0)
        assert(false);
      ::fread(buffer.data(), length, 1, query_file_string);
      reqs_.emplace_back(std::string(buffer.data(), buffer.data() + length));
    }
    num_of_reqs_unique_ = reqs_.size();
    LOG(INFO) << "Number of query = " << num_of_reqs_unique_;
  }

  void do_query(size_t thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> rnd(0, oids_.size());

    std::vector<char> request_char(sizeof(int64_t) + 1);
    request_char[sizeof(int64_t)] = static_cast<char>(31);

    size_t person_object_id;
    // size_t person_ids[9] = {19791209384079, 30786325760991, 32985348965767,
    //                         10995116408126, 4398046550239,  6597069777568,
    //                         17592186090538, 13194139696778, 2199023390176};
    // size_t idx_tmp = 0;
    size_t id;
    while (true) {
      id = cur_.fetch_add(1);
      if (id >= num_of_reqs_) {
        break;
      }
      // graph.get_lid(person_label_id, 21582, (gs::vid_t&) person_vertex_id);
      person_object_id = oids_[rnd(gen) % oids_.size()];
      // if (idx_tmp == 9)
      //   return;
      // person_object_id = person_ids[idx_tmp++];
      memcpy(request_char.data(), &person_object_id, sizeof(int64_t));

      std::string request_str = {request_char.data(), request_char.size()};
      gbp::get_query_id().store(id);
      start_[id] = gbp::GetSystemTime();
      // if (gbp::warmup_mark() == 1)
      //   gbp::get_thread_logfile() << gbp::GetSystemTime() << " start "
      //                             << person_object_id << std::endl;

      auto ret = gs::GraphDB::get().GetSession(thread_id).Eval(request_str);
      // LOG(INFO) << person_object_id;
      // LOG(INFO) << std::string_view{ret.data(), ret.size()};
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
      auto tmp = end_[idx] - start_[idx];
      ts[0].emplace_back(tmp);
      vec[0] += tmp;
      count[0] += 1;
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

    size_t sum = std::accumulate(vec.begin(), vec.begin() + 21, (size_t) 0);
    std::cout << "Latency SUM = " << sum << std::endl;
    std::cout << "unit: CPU Cycles\n";
  }
  void LoggerStop() {
    logger_stop_ = true;
    if (log_thread_.joinable())
      log_thread_.join();
  }

 private:
  Req() : cur_(0), warmup_num_(0) {}
  ~Req() {
    logger_stop_ = true;
    if (log_thread_.joinable())
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
  std::vector<size_t> run_time_req_ids_;

  // std::vector<std::chrono::system_clock::time_point> start_;
  // std::vector<std::chrono::system_clock::time_point> end_;
  std::vector<size_t> start_;
  std::vector<size_t> end_;

  std::thread log_thread_;
  std::atomic<bool> logger_stop_ = false;
  // std::vector<executor_ref> executor_refs_;

  std::vector<gs::oid_t> oids_;
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
  size_t pool_num = 8;
  size_t io_server_num = 2;
  gbp::warmup_mark().store(0);

  if (vm.count("buffer-pool-size")) {
    pool_size_Byte = vm["buffer-pool-size"].as<uint64_t>();
  }
  LOG(INFO) << "pool_size_Byte = " << pool_size_Byte << " Bytes";
  gbp::BufferPoolManager::GetGlobalInstance().init(
      pool_num, CEIL(pool_size_Byte, gbp::PAGE_SIZE_MEMORY) / pool_num,
      io_server_num);

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
  gbp::BufferPoolManager::GetGlobalInstance().Clean();
  LOG(INFO) << "Clean finish";
#else
  gbp::warmup_mark().store(1);
  LOG(INFO) << "Clean start";
  gbp::CleanMAS();
  LOG(INFO) << "Clean finish";
#endif
  gbp::warmup_mark().store(1);

  std::string req_file = vm["req-file"].as<std::string>();
  // Req::get().load_query(req_file);
  // Req::get().load_result(req_file);
  gbp::DirectCache::CleanAllCache();
  for (size_t idx = 0; idx < 1; idx++) {
    Req::get().init(warmup_num, benchmark_num);
    // gbp::BufferPoolManager::GetGlobalInstance().disk_manager_->ResetCount();
    // hiactor::actor_app app;
    gbp::log_enable().store(true);
    sleep(1);
    size_t ssd_io_byte = std::get<0>(gbp::SSD_io_bytes());
    auto cpu_cost_before = gbp::GetCPUTime();

    auto begin = std::chrono::system_clock::now();

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

    gbp::warmup_mark().store(0);
    std::cout << "cost time:"
              << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       begin)
                     .count()
              << "\n";
    Req::get().output();

    gbp::warmup_mark().store(1);
    gbp::DirectCache::CleanAllCache();
  }

  Req::get().LoggerStop();
  auto memory_usages =
      gbp::BufferPoolManager::GetGlobalInstance().GetMemoryUsage();

  LOG(INFO) << std::get<0>(memory_usages) << " | " << std::get<1>(memory_usages)
            << " | " << std::get<2>(memory_usages) << " | "
            << std::get<3>(memory_usages) << " | "
            << std::get<4>(memory_usages);
}