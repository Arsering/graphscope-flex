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

#include <filesystem>
#include <iostream>

#include <glog/logging.h>

#include <boost/program_options.hpp>

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/http_server/options.h"

namespace bpo = boost::program_options;

int main(int argc, char** argv) {
  size_t pool_size_Byte = 1024LU * 1024LU * 1024LU * 50;

  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("parallelism,p",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "parallelism of bulk loader")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "graph-config,g", bpo::value<std::string>(), "graph schema config file")(
      "bulk-load,l", bpo::value<std::string>(), "bulk-load config file")(
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

  uint32_t parallelism = vm["parallelism"].as<uint32_t>();
  std::string data_path = "";
  std::string bulk_load_config_path = "";
  std::string graph_schema_path = "";

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
  if (!vm.count("bulk-load")) {
    LOG(ERROR) << "bulk-load-config is required";
    return -1;
  }
  bulk_load_config_path = vm["bulk-load"].as<std::string>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  // LOG(INFO) << "Launch Performance Logger";
  // if (!vm.count("log-data-path")) {
  //   LOG(ERROR) << "log-data-path is required";
  //   return -1;
  // }
  // auto log_data_path = vm["log-data-path"].as<std::string>();
  // gbp::PerformanceLogServer::GetPerformanceLogger().Start(
  //     log_data_path + "/performance.log", "nvme0n1");
  // gbp::get_log_dir() = log_data_path;

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  auto loading_config =
      gs::LoadingConfig::ParseFromYaml(schema, bulk_load_config_path);

  std::filesystem::path data_dir_path(data_path);
  if (!std::filesystem::exists(data_dir_path)) {
    std::filesystem::create_directory(data_dir_path);
  }
  std::filesystem::path serial_path = data_dir_path / "schema";
  if (std::filesystem::exists(serial_path)) {
    LOG(ERROR) << "data directory is not empty";
    return -1;
  }

  double t0 = -grape::GetCurrentTime();
#if !OV
  size_t pool_num = 1;
  if (vm.count("buffer-pool-size")) {
    pool_size_Byte = vm["buffer-pool-size"].as<uint64_t>();
  }

  LOG(INFO) << "pool_size_Byte = " << pool_size_Byte << " Bytes";

  gbp::BufferPoolManager::GetGlobalInstance().init(
      pool_num, CEIL(CEIL(pool_size_Byte, gbp::PAGE_SIZE_MEMORY), pool_num),
      pool_num);

  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Finished initializing BufferPoolManager, elapsed " << t0
            << " s";
  assert(gbp::PERSISTENT);
  t0 = -grape::GetCurrentTime();
#endif

  auto loader = gs::LoaderFactory::CreateFragmentLoader(
      data_dir_path.string(), schema, loading_config, parallelism);
  loader->LoadFragment();

  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Finished reloading Graph, elapsed " << t0 << " s";
  return 0;
}