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

#include <glog/logging.h>
#include <boost/program_options.hpp>
#include <iostream>
#include <seastar/core/alien.hh>

#include "flex/engines/graph_db/database/graph_db_session.h"

namespace bpo = boost::program_options;
using namespace std::chrono_literals;

class CSVReader {
  std::ifstream csv_data_;
  std::vector<std::string> words_;

 public:
  CSVReader(const std::string& file_name) {
    csv_data_.open(file_name, std::ios::in);
    LOG(INFO) << "file_name=" << file_name;
    std::string line;
    std::getline(csv_data_, line);  // ignore the first line of file
  }
  ~CSVReader() = default;
  std::vector<std::string>& GetNextLine() {
    words_.clear();
    std::string line, word;
    if (!std::getline(csv_data_, line))
      return words_;

    std::istringstream sin;
    sin.clear();
    sin.str(line);

    while (std::getline(sin, word, '|')) {
      words_.push_back(word);
    }
    return words_;
  }
};

int test_vertex(gs::GraphDB& db, const std::string& csv_data_path) {
  gs::GraphDBSession& graph_session = db.GetSession(0);
  gs::label_t person_label_id =
      graph_session.schema().get_vertex_label_id("PERSON");
  gs::StringColumn& person_firstName_col =
      *(std::dynamic_pointer_cast<gs::StringColumn>(
          graph_session.get_vertex_property_column(person_label_id,
                                                   "firstName")));
  gs::StringColumn& person_email_col =
      *(std::dynamic_pointer_cast<gs::StringColumn>(
          graph_session.get_vertex_property_column(person_label_id, "email")));
  LOG(INFO) << "csv_data_path=" << csv_data_path;
  CSVReader csv_reader(csv_data_path + "/dynamic/person_0_0.csv");

  size_t test_count = 100;
  gs::oid_t req = 933;
  gs::vid_t root{};
  auto txn = graph_session.GetReadTransaction();
  uint64_t t0;

#if OV
  while (test_count > 0) {
    auto& words = csv_reader.GetNextLine();
    req = stol(words[0]);
    LOG(INFO) << gbp::GetSystemTime();
    txn.GetVertexIndex(person_label_id, req, root);
    LOG(INFO) << gbp::GetSystemTime();
    const auto email = person_email_col.get_view(root);
    LOG(INFO) << gbp::GetSystemTime();
    const auto firstname = person_firstName_col.get_view(root);
    LOG(INFO) << gbp::GetSystemTime();
    LOG(INFO) << "==============";
    if (!(firstname.compare(words[1]) == 0 && email.compare(words[9]) == 0)) {
      LOG(INFO) << firstname << " | " << email;
      LOG(FATAL) << "test_vertex: failed!!";
    }
    test_count--;
  }
#else
  while (test_count > 0) {
    auto& words = csv_reader.GetNextLine();
    req = stol(words[0]);

    LOG(INFO) << gbp::GetSystemTime();
    txn.GetVertexIndex(person_label_id, req, root);
    LOG(INFO) << gbp::GetSystemTime();
    const auto email = person_email_col.get(root);
    LOG(INFO) << gbp::GetSystemTime();
    const auto firstname = person_firstName_col.get(root);
    LOG(INFO) << gbp::GetSystemTime();
    LOG(INFO) << "==============";

    // std::string str1{firstname.Data(), firstname.Size()};
    // std::string str2{email.Data(), email.Size()};
    if (!(firstname == words[1] && email == words[9])) {
      // LOG(INFO) << str1 << " | " << str2;
      LOG(FATAL) << "test_vertex: failed!!";
    }
    test_count--;
  }
#endif

  // LOG(INFO) << "test_vertex: success!! time = " << t0;
  return 0;
}

int test_edge(gs::GraphDB& db, const std::string& csv_data_path,
              const std::string& log_data_path) {
  gs::GraphDBSession& graph_session = db.GetSession(0);

  gs::label_t person_label_id =
      graph_session.schema().get_vertex_label_id("PERSON");
  gs::label_t knows_label_id =
      graph_session.schema().get_edge_label_id("KNOWS");

  CSVReader csv_reader(csv_data_path + "/dynamic/person_knows_person_0_0.csv");
  gs::oid_t req = 17592186045360;
  gs::vid_t root{};
  auto txn = graph_session.GetReadTransaction();

  if (!txn.GetVertexIndex(person_label_id, req, root)) {
    return false;
  }
  LOG(INFO) << "root = " << root;

  auto oe = txn.GetOutgoingEdges<gs::Date>(person_label_id, root,
                                           person_label_id, knows_label_id);

  LOG(INFO) << "oe.size = " << oe.estimated_degree();

  std::ofstream log_file;
  std::vector<std::string> vec_date;
#if OV
  log_file.open(log_data_path + "/o_output.log", std::ios::out);
  for (auto& e : oe) {
    vec_date.emplace_back(e.data.to_string());
    LOG(INFO) << vec_date[vec_date.size() - 1];
    // log_file << vec_date[vec_date.size() - 1] << std::endl;
  }
#else
  log_file.open(log_data_path + "/n_output.log", std::ios::out);
  for (; oe.is_valid(); oe.next()) {
    auto item = oe.get_data();
    // vec_date.emplace_back(gbp::BufferObject::Ref<gs::Date>(item).to_string());
    assert(false);
    LOG(INFO) << vec_date[vec_date.size() - 1];
    log_file << vec_date[vec_date.size() - 1] << std::endl;
  }
#endif

  LOG(INFO) << "test_edge: success!!";
  return 0;
}

int main(int argc, char** argv) {
  bpo::options_description desc("Usage:");
  desc.add_options()("help", "Display help message")(
      "version,v", "Display version")("shard-num,s",
                                      bpo::value<uint32_t>()->default_value(1),
                                      "shard number of actor system")(
      "http-port,p", bpo::value<uint16_t>()->default_value(10000),
      "http port of query handler")("graph-config,g", bpo::value<std::string>(),
                                    "graph schema config file")(
      "data-path,d", bpo::value<std::string>(), "data directory path")(
      "csv-data-path,c", bpo::value<std::string>(), "csv data directory path")(
      "log-data-path,l", bpo::value<std::string>(), "log data directory path");

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
  std::string csv_data_path = "";
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
  if (!vm.count("csv-data-path")) {
    LOG(ERROR) << "csv-data-path is required";
    return -1;
  }
  csv_data_path = vm["csv-data-path"].as<std::string>();
  if (!vm.count("log-data-path")) {
    LOG(ERROR) << "log-data-path is required";
    return -1;
  }
  log_data_path = vm["log-data-path"].as<std::string>();

  setenv("TZ", "Asia/Shanghai", 1);
  tzset();

  size_t pool_size = 1024 * 1024 * 16;

  gbp::BufferPoolManager::GetGlobalInstance().init(200, pool_size, 200);

  // gbp::BufferPoolManager::GetGlobalInstance().init(pool_size);

  double t0 = -grape::GetCurrentTime();
  auto& db = gs::GraphDB::get();

  auto schema = gs::Schema::LoadFromYaml(graph_schema_path);
  LOG(INFO) << "Start loading graph";
  // gbp::set_start_log(false);
  db.Init(schema, data_path, shard_num);
  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Finished loading graph, elapsed " << t0 << " s";

  test_vertex(db, csv_data_path);
  // test_edge(db, csv_data_path, log_data_path);
}