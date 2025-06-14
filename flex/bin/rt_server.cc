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

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/graph_db_service.h"
#include "flex/engines/http_server/options.h"

#include <boost/program_options.hpp>
#include <seastar/core/alien.hh>

#include <glog/logging.h>

using namespace server;
namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  size_t pool_size_Byte = 1024LU * 1024LU * 8;

  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("shard-num,s",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "shard number of actor system")(
      "http-port,p", bpo::value<uint16_t>()->default_value(10000),
      "http port of query handler")("graph-config,g", bpo::value<std::string>(),
                                    "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
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
  uint16_t http_port = vm["http-port"].as<uint16_t>();

  std::string graph_schema_path = "";
  std::string data_path = "";
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
      log_data_path + "/performance.log", "md0");
  gbp::get_db_dir() = data_path;

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  double t0 = -grape::GetCurrentTime();
#if !OV
  size_t pool_num = 8;
  size_t io_server_num = 4;

  if (vm.count("buffer-pool-size")) {
    pool_size_Byte = vm["buffer-pool-size"].as<uint64_t>();
  }
  LOG(INFO) << "pool_size_Byte = " << pool_size_Byte << " Bytes";
  gbp::BufferPoolManager::GetGlobalInstance().init(
      pool_num, CEIL(pool_size_Byte, gbp::PAGE_SIZE_MEMORY) / pool_num,
      io_server_num);

  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Finished initializing BufferPoolManager, elapsed " << t0
            << " s";

  t0 = -grape::GetCurrentTime();
#endif

  auto& db = gs::GraphDB::get();
  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);

  // init access logger
  gbp::get_log_dir() = log_data_path;
  db.Init(schema, data_path, shard_num);

  t0 += grape::GetCurrentTime();

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
#endif

#if !OV
  t0 = -grape::GetCurrentTime();

  LOG(INFO) << "Warmup start";
  // gbp::BufferPoolManager::GetGlobalInstance().WarmUp();
  LOG(INFO) << "Warmup finish";
  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Finished BufferPool warm up, elapsed " << t0 << " s";

  LOG(INFO) << "Clean start";
  // gbp::BufferPoolManager::GetGlobalInstance().Clean();
  LOG(INFO) << "Clean finish";
#else
  LOG(INFO) << "Clean start";
  // gbp::CleanMAS();
  LOG(INFO) << "Clean finish";
#endif

  gbp::PerformanceLogServer::GetPerformanceLogger().SetStartPoint();
  gbp::warmup_mark().store(1);
  // start service
  LOG(INFO) << "GraphScope http server start to listen on port " << http_port;
  server::GraphDBService::get().init(shard_num, http_port, enable_dpdk);

  server::GraphDBService::get().run_and_wait_for_exit();

  return 0;
}
