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
#include <boost/program_options.hpp>
#include <chrono>
#include <fstream>
#include <hiactor/core/actor-app.hh>
#include <iostream>
#include <thread>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/executor_group.actg.h"
#include "flex/engines/http_server/generated/executor_ref.act.autogen.h"
#include "flex/engines/http_server/graph_db_service.h"

#include <glog/logging.h>

namespace bpo = boost::program_options;
using namespace std::chrono_literals;
class Req {
 public:
  static Req& get() {
    static Req r;
    return r;
  }
  void init(size_t warmup_num, size_t benchmark_num) {
    warmup_num_ = warmup_num;
    num_of_reqs_ = warmup_num + benchmark_num;

    // num_of_reqs_unique_ = 2000000;
    start_.resize(num_of_reqs_);
    end_.resize(num_of_reqs_);
    cur_ = 0;

    LOG(INFO) << "warmup count: " << warmup_num_
              << "; benchmark count: " << num_of_reqs_ << "\n";

    run_time_req_ids_.resize(num_of_reqs_);
    std::iota(run_time_req_ids_.begin(), run_time_req_ids_.end(), 0);
    std::random_device rd;
    std::mt19937 rng(rd());
    std::shuffle(run_time_req_ids_.begin(), run_time_req_ids_.end(), rng);

    if (!log_thread_.joinable())
      log_thread_ = std::thread([this]() { logger(); });
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

  seastar::future<> do_query(server::executor_ref& ref) {
    auto id = cur_.fetch_add(1);

    if (id >= num_of_reqs_) {
      return seastar::make_ready_future<>();
    }

    start_[id] = gbp::GetSystemTime();
    gbp::get_query_id().store(id % num_of_reqs_unique_);

    return ref
        .run_graph_db_query(
            server::query_param{reqs_[id % num_of_reqs_unique_]})
        .then_wrapped(
            [&, id](seastar::future<server::query_result>&& fut) mutable {
              auto result = fut.get0();
              end_[id] = gbp::GetSystemTime();
            })
        .then([&] { return do_query(ref); });
  }

  seastar::future<> simulate() {
    hiactor::scope_builder builder;
    builder.set_shard(hiactor::local_shard_id())
        .enter_sub_scope(hiactor::scope<server::executor_group>(0));
    return seastar::do_with(
        builder.build_ref<server::executor_ref>(0),
        [&](server::executor_ref& ref) { return do_query(ref); });
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
  std::vector<size_t> run_time_req_ids_;

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
#else
  gbp::warmup_mark().store(1);
#endif

  std::string req_file = vm["req-file"].as<std::string>();
  Req::get().load_query(req_file);
  // Req::get().load_result(req_file);
  for (size_t idx = 0; idx < 1; idx++) {
    Req::get().init(warmup_num, benchmark_num);

    hiactor::actor_app app;
    gbp::log_enable().store(true);

    auto begin = std::chrono::system_clock::now();
    int ac = 1;
    char* av[] = {(char*) "rt_bench"};
    app.run(ac, av, [shard_num] {
      return seastar::parallel_for_each(
                 boost::irange<unsigned>(0u, shard_num),
                 [](unsigned id) {
                   return seastar::smp::submit_to(
                       id, [id] { return Req::get().simulate(); });
                 })
          .then([] {
            hiactor::actor_engine().exit();
            fmt::print("Exit actor system.\n");
          });
    });
    auto end = std::chrono::system_clock::now();
    gbp::warmup_mark().store(0);

    std::cout << "cost time:"
              << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       begin)
                     .count()
              << "\n";
    Req::get().LoggerStop();
    Req::get().output();
  }
}