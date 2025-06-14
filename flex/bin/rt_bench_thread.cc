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
#include <iostream>
#include <thread>
#include <vector>
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/hqps_db/database/mutable_csr_interface.h"

#include <glog/logging.h>

namespace bpo = boost::program_options;

void pre_compute_comment(const std::string dir_path) {
  gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
  auto comment_label_id = graph.schema().get_vertex_label_id("COMMENT");
  auto replyOf_label_id = graph.schema().get_edge_label_id("REPLYOF");
  auto comment_replyOf_comment_in =
      dynamic_cast<gs::MutableCsr<grape::EmptyType>*>(graph.get_ie_csr(
          comment_label_id, comment_label_id, replyOf_label_id));
  gs::vid_t vertex_id;
  for (size_t range = 0; range < 10; range += 2) {
    std::string file_path = dir_path + "/filtered_sets/comment/comment_" +
                            std::to_string(range) + "_" +
                            std::to_string(range + 2);
    LOG(INFO) << file_path;
    std::ifstream file(file_path);  // 打开文件
    if (!file.is_open()) {
      LOG(INFO) << "Unable to open file" << std::endl;
      assert(false);
    }

    std::string line;
    gs::oid_t comment_id = 0;

    // 按行读取文件
    while (std::getline(file, line)) {
      std::istringstream iss(line);
      size_t number;
      if (!(iss >> number)) {  // 尝试将行内容转换为整数
        std::cerr << "Invalid number in file" << std::endl;
        continue;  // 如果转换失败，跳过当前行
      }
      comment_id = number;

      graph.get_lid(comment_label_id, comment_id, vertex_id);
      // comment_replyOf_comment_in->copy_before_insert(vertex_id);
    }
    file.close();  // 关闭文件
  }
}

void pre_compute_post(const std::string dir_path) {
  gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
  auto comment_label_id = graph.schema().get_vertex_label_id("COMMENT");
  auto post_label_id = graph.schema().get_vertex_label_id("POST");
  auto replyOf_label_id = graph.schema().get_edge_label_id("REPLYOF");
  auto comment_replyOf_post_in =
      dynamic_cast<gs::MutableCsr<grape::EmptyType>*>(
          graph.get_ie_csr(post_label_id, comment_label_id, replyOf_label_id));

  gs::vid_t vertex_id = 120;
  for (size_t range = 0; range < 10; range += 2) {
    std::string file_path = dir_path + "/filtered_sets/post/post_" +
                            std::to_string(range) + "_" +
                            std::to_string(range + 2);
    LOG(INFO) << file_path;
    std::ifstream file(file_path);  // 打开文件
    if (!file.is_open()) {
      LOG(INFO) << "Unable to open file" << std::endl;
      assert(false);
    }

    std::string line;
    gs::oid_t post_id;
    // 按行读取文件
    while (std::getline(file, line)) {
      std::istringstream iss(line);
      size_t number;
      if (!(iss >> number)) {  // 尝试将行内容转换为整数
        std::cerr << "Invalid number in file" << std::endl;
        continue;  // 如果转换失败，跳过当前行
      }
      post_id = number;
      graph.get_lid(post_label_id, post_id, vertex_id);
      // comment_replyOf_post_in->copy_before_insert(vertex_id);
    }
    file.close();  // 关闭文件
  }
}

void pre_compute_forum(const std::string dir_path) {
  gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
  auto comment_label_id = graph.schema().get_vertex_label_id("FORUM");
  auto post_label_id = graph.schema().get_vertex_label_id("POST");
  auto replyOf_label_id = graph.schema().get_edge_label_id("REPLYOF");
  auto comment_replyOf_post_in =
      dynamic_cast<gs::MutableCsr<grape::EmptyType>*>(
          graph.get_ie_csr(post_label_id, comment_label_id, replyOf_label_id));

  gs::vid_t vertex_id = 120;
  for (size_t range = 0; range < 2; range += 2) {
    std::string file_path = dir_path + "/filtered_sets/post/post_" +
                            std::to_string(range) + "_" +
                            std::to_string(range + 2);
    LOG(INFO) << file_path;
    std::ifstream file(file_path);  // 打开文件
    if (!file.is_open()) {
      LOG(INFO) << "Unable to open file" << std::endl;
      assert(false);
    }

    std::string line;
    gs::oid_t post_id;
    // 按行读取文件
    while (std::getline(file, line)) {
      std::istringstream iss(line);
      size_t number;
      if (!(iss >> number)) {  // 尝试将行内容转换为整数
        std::cerr << "Invalid number in file" << std::endl;
        continue;  // 如果转换失败，跳过当前行
      }
      post_id = number;
      graph.get_lid(post_label_id, post_id, vertex_id);
      // comment_replyOf_post_in->copy_before_insert(vertex_id);
    }
    file.close();  // 关闭文件
  }
}
#if !OV
class BatchTest {
 public:
  // 测试BatchGetVertexIds接口
  void test_BatchGetVertexIds() {
    gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
    auto person_label_id = graph.schema().get_vertex_label_id("PERSON");
    auto txn = gs::GraphDB::get().GetSession(0).GetReadTransaction();
    txn.BatchGetVertexIds(person_label_id, {1, 2, 3});
    LOG(INFO) << "BatchGetVertexIds: Test Success";
  }

  // 测试BatchGetVertexIndices接口
  void test_BatchGetVertexIndices() {
    gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
    auto person_label_id = graph.schema().get_vertex_label_id("PERSON");
    auto txn = gs::GraphDB::get().GetSession(0).GetReadTransaction();
    txn.BatchGetVertexIndices(person_label_id, {1129, 4194, 8333});
    LOG(INFO) << "BatchGetVertexIndices: Test Success";
  }

  // 测试BatchGetOutgoingEdges接口
  void test_BatchGetOutgoingEdges() {
    gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
    auto person_label_id = graph.schema().get_vertex_label_id("PERSON");
    auto knows_label_id = graph.schema().get_edge_label_id("KNOWS");

    auto txn = gs::GraphDB::get().GetSession(0).GetReadTransaction();
    std::vector<gs::vid_t> vids = {1, 2, 3};
    auto results = txn.BatchGetOutgoingEdges<gs::Date>(
        person_label_id, person_label_id, knows_label_id, vids);

    auto person_knows_person_out = txn.GetOutgoingGraphView<gs::Date>(
        person_label_id, person_label_id, knows_label_id);
    for (size_t i = 0; i < results.size(); i++) {
      auto edges = person_knows_person_out.get_edges(vids[i]);
      assert(results[i].estimated_degree() ==
             edges.estimated_degree());  // 检查度数是否一致

      assert(results[i].is_valid() ==
             edges.is_valid());  // 检查第一个邻居是否有效
      if (results[i].is_valid()) {
        assert(results[i].get_neighbor() ==
               edges.get_neighbor());  // 检查第一个邻居是否一致
      }

      results[i].next();
      edges.next();
      assert(results[i].is_valid() ==
             edges.is_valid());  // 检查第二个邻居是否有效
      if (results[i].is_valid()) {
        assert(results[i].get_neighbor() ==
               edges.get_neighbor());  // 检查第二个邻居是否一致
      }
    }
    LOG(INFO) << "BatchGetOutgoingEdges: Test Success";
  }

  // 测试BatchGetIncomingEdges接口
  void test_BatchGetIncomingEdges() {
    gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
    auto person_label_id = graph.schema().get_vertex_label_id("PERSON");
    auto knows_label_id = graph.schema().get_edge_label_id("KNOWS");

    auto txn = gs::GraphDB::get().GetSession(0).GetReadTransaction();
    std::vector<gs::vid_t> vids = {1, 2, 3};
    auto results = txn.BatchGetIncomingEdges<gs::Date>(
        person_label_id, person_label_id, knows_label_id, vids);

    auto person_knows_person_in = txn.GetIncomingGraphView<gs::Date>(
        person_label_id, person_label_id, knows_label_id);
    for (size_t i = 0; i < results.size(); i++) {
      auto edges = person_knows_person_in.get_edges(vids[i]);
      assert(results[i].estimated_degree() ==
             edges.estimated_degree());  // 检查度数是否一致

      assert(results[i].is_valid() ==
             edges.is_valid());  // 检查第一个邻居是否有效
      if (results[i].is_valid()) {
        assert(results[i].get_neighbor() ==
               edges.get_neighbor());  // 检查第一个邻居是否一致
      }

      results[i].next();
      edges.next();
      assert(results[i].is_valid() ==
             edges.is_valid());  // 检查第二个邻居是否有效
      if (results[i].is_valid()) {
        assert(results[i].get_neighbor() ==
               edges.get_neighbor());  // 检查第二个邻居是否一致
      }
    }
    LOG(INFO) << "BatchGetOutgoingEdges: Test Success";
  }

  // 测试BatchGetVertexPropsFromVid接口
  void test_BatchGetVertexPropsFromVid() {
    gs::MutablePropertyFragment& graph = gs::GraphDB::get().graph();
    auto person_label_id = graph.schema().get_vertex_label_id("PERSON");
    auto txn = gs::GraphDB::get().GetSession(0).GetReadTransaction();
    std::vector<gs::vid_t> vids = {1, 25556, 25557, 25558, 25559, 75559, 3, 4};
    std::vector<std::string> person_property_names = {"creationDate",
                                                      "browserUsed"};

    auto results = txn.BatchGetVertexPropsFromVids(person_label_id, vids,
                                                   person_property_names);
    auto person_creationDate_col = std::dynamic_pointer_cast<gs::DateColumn>(
        gs::GraphDB::get().GetSession(0).get_vertex_property_column(
            person_label_id, "creationDate"));
    auto person_browserUsed_col = std::dynamic_pointer_cast<gs::StringColumn>(
        gs::GraphDB::get().GetSession(0).get_vertex_property_column(
            person_label_id, "browserUsed"));

    for (size_t i = 0; i < vids.size(); i++) {
      auto creationDate = person_creationDate_col->get(vids[i]);
      auto browserUsed = person_browserUsed_col->get(vids[i]);
      assert(creationDate == results[0][i]);
      assert(browserUsed == results[1][i]);
    }
    LOG(INFO) << "BatchGetVertexPropsFromVid: Test Success";
  }
};
#endif

class Req {
 public:
  static Req& get() {
    static Req r;
    return r;
  }

  void init(size_t warmup_num, size_t benchmark_num) {
    warmup_num_ = warmup_num;
    num_of_reqs_ = warmup_num + benchmark_num;

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
        if (gbp::get_results_vec().size() == num_of_reqs_) {
          break;
        }
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
    int count = 0;

    while (true) {
      auto ret = ::fread(&length, sizeof(size_t), 1, query_file_string_view);
      if (ret == 0)
        break;

      if (length == 0)
        assert(false);
      ::fread(buffer.data(), length, 1, query_file_string);
      // // uint8_t type = buffer.back();
      // auto req_string = std::string(buffer.data(), buffer.data() + length);
      // uint8_t type = req_string.back();
      // if ((int)type == 2) {
      //   reqs_.emplace_back(req_string);
      //   count++;
      // }
      reqs_.emplace_back(std::string(buffer.data(), buffer.data() + length));
      if (reqs_.size() == num_of_reqs_) {
        break;
      }
      // if (count == num_of_reqs_) {
      //   break;
      // }
    }
    num_of_reqs_unique_ = reqs_.size();
    LOG(INFO) << "Number of query = " << num_of_reqs_unique_;
  }

  void do_query(size_t thread_id) {
    size_t id;

    while (true) {
      id = cur_.fetch_add(1);

      if (id >= num_of_reqs_unique_) {
        // LOG(INFO) << gbp::get_thread_id();
        break;
      }
      // id = run_time_req_ids_[id];

      start_[id] = gbp::GetSystemTime();
      gbp::get_query_id().store(id % num_of_reqs_unique_);

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
  void do_update_query(size_t thread_id) {
    size_t id;
    gbp::get_thread_logfile();

    while (true) {
      id = update_cur_.fetch_add(1);

      // 检查是否已经完成所有更新
      if (id >= num_of_update_reqs_unique_) {
        break;
      }

      // 等待read完成上一轮的read_limit条查询
      while (id > 0 && (id / update_limit) > (read_cur_.load() / read_limit)) {
        if (read_cur_.load() >= num_of_reqs_) {
          return;
        }
      }

      start_[id] = gbp::GetSystemTime();
      // LOG(INFO)<<"update";
      gbp::get_query_id().store(id % num_of_update_reqs_unique_);
      auto ret = gs::GraphDB::get().GetSession(thread_id).Eval(
          update_list[id % num_of_update_reqs_unique_]);
      end_[id] = gbp::GetSystemTime();
    }
    return;
  }

  void do_read_query(size_t thread_id) {
    size_t id;
    gbp::get_thread_logfile();

    while (true) {
      id = read_cur_.fetch_add(1);

      if (id >= num_of_reqs_) {
        break;
      }

      // 等待update完成当前轮次的update_limit条更新
      while ((id / read_limit) >= (update_cur_.load() / update_limit)) {
        if (update_cur_.load() >= num_of_update_reqs_unique_) {
          break;
        }
      }

      start_[id] = gbp::GetSystemTime();
      gbp::get_query_id().store(id % num_of_read_reqs_unique_);
      // LOG(INFO)<<"read";
      auto ret = gs::GraphDB::get().GetSession(thread_id).Eval(
          read_list[id % num_of_read_reqs_unique_]);
      end_[id] = gbp::GetSystemTime();
    }
    return;
  }

  bool simulate_with_update_and_read(size_t thread_num = 10) {
    shard_num = thread_num - 1;
    std::vector<std::thread> workers;
    assert(cur_ == 0);

    workers.emplace_back([this]() { do_update_query(0); });

    for (size_t i = 1; i < thread_num; i++) {
      workers.emplace_back([this, i]() { do_read_query(i); });
    }

    for (auto& worker : workers) {
      worker.join();
    }
    return true;
  }

  void load_query_with_timestamp(const std::string& file_path) {
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
    auto test_count = 0;
    while (true) {
      auto ret = ::fread(&length, sizeof(size_t), 1, query_file_string_view);
      if (ret == 0)
        break;

      if (length == 0)
        assert(false);
      size_t timestamp = 0;
      ::fread(&timestamp, sizeof(size_t), 1, query_file_string);
      // std::cout<<timestamp<<std::endl;
      size_t data_len = length - sizeof(size_t);
      ::fread(buffer.data(), data_len, 1, query_file_string);
      auto req = std::string(buffer.data(), buffer.data() + data_len);
      // reqs_.emplace_back(std::string(buffer.data(), buffer.data() +
      // data_len));
      if (req.back() <= 21) {
        assert(req.back() != 0);
        if (read_list.size() >= 1000000) {
          continue;
        }
        read_list.emplace_back(req);
      } else {
        update_list.emplace_back(req);
      }
    }
    num_of_reqs_unique_ = update_list.size() + read_list.size();
    num_of_read_reqs_unique_ = read_list.size();
    num_of_update_reqs_unique_ = update_list.size();
    // read_limit=read_list.size()/100;
    // update_limit=update_list.size()/100;
    read_limit = 2000000;
    update_limit = update_list.size() / 100;
    // update_limit=10;
    // read_limit=10;
    LOG(INFO) << "Number of query = " << num_of_reqs_unique_
              << " \nNumber of read query = " << num_of_read_reqs_unique_
              << " \nNumber of update query = " << num_of_update_reqs_unique_;
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
      // gbp::get_thread_logfile()
      //     << idx << " | " << id << " | " << tmp << std::endl;

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
  std::atomic<size_t> update_cur_{0};
  std::atomic<size_t> read_cur_{0};
  std::atomic<bool> update_epoch_flag{false};  // 更新周期标记
  std::atomic<size_t> read_epoch_counter{0};   // 读操作的计数器

  unsigned long long read_limit = 100000;
  unsigned long long update_limit = 100000;

  size_t num_of_read_reqs_unique_;
  size_t num_of_update_reqs_unique_;
  size_t shard_num;

  size_t warmup_num_;
  size_t num_of_reqs_;
  size_t num_of_reqs_unique_;

  std::vector<std::string> reqs_;
  std::vector<std::string> update_list;
  std::vector<std::string> read_list;
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
      log_data_path + "/performance.log", "md0");
  gbp::get_log_dir() = log_data_path;
  gbp::get_db_dir() = data_path;

  gbp::log_enable().store(false);
  setenv("TZ", "Asia/Shanghai", 1);
  tzset();
#if OV
#else
  size_t pool_num = 16;
  size_t io_server_num = 8;

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
  gbp::CleanMAS();
  LOG(INFO) << "Clean finish";
#endif
  gbp::warmup_mark().store(1);

  // {
  //   BatchTest test;
  //   test.test_BatchGetVertexIds();
  //   test.test_BatchGetOutgoingEdges();
  //   test.test_BatchGetIncomingEdges();
  //   test.test_BatchGetVertexPropsFromVid();
  //   // return 1;
  // }

  std::string req_file = vm["req-file"].as<std::string>();
  Req::get().init(warmup_num, benchmark_num);
  Req::get().load_query(req_file);
  // Req::get().load_query_with_timestamp(req_file);
  // Req::get().load_result(req_file);
  gbp::DirectCache::CleanAllCache();
  // pre_compute_post(data_path);
  // pre_compute_comment(data_path);

  for (size_t idx = 0; idx < 2; idx++) {
    gbp::get_counter_global(9) = 0;
    gbp::get_counter_global(10) = 0;
    gbp::get_counter_global(11) = 0;
    gbp::get_counter_global(12) = 0;

    gbp::PerformanceLogServer::GetPerformanceLogger().SetStartPoint();

    Req::get().init(warmup_num, benchmark_num);
    // gbp::BufferPoolManager::GetGlobalInstance().disk_manager_->ResetCount();
    // hiactor::actor_app app;
    gbp::log_enable().store(true);
    sleep(1);
    size_t ssd_io_r_byte = std::get<0>(gbp::SSD_io_bytes());
    size_t ssd_io_w_byte = std::get<1>(gbp::SSD_io_bytes());

    auto cpu_cost_before = gbp::GetCPUTime();

    auto begin = std::chrono::system_clock::now();

    Req::get().simulate(shard_num);
    // Req::get().simulate_with_update_and_read(shard_num);
    auto end = std::chrono::system_clock::now();
    auto cpu_cost_after = gbp::GetCPUTime();

    ssd_io_r_byte = std::get<0>(gbp::SSD_io_bytes()) - ssd_io_r_byte;
    ssd_io_w_byte = std::get<1>(gbp::SSD_io_bytes()) - ssd_io_w_byte;

    LOG(INFO) << "CPU Cost = "
              << (std::get<0>(cpu_cost_after) - std::get<0>(cpu_cost_before)) /
                     1000000.0
              << "u/"
              << (std::get<1>(cpu_cost_after) - std::get<1>(cpu_cost_before)) /
                     1000000.0
              << "s (second)";
    LOG(INFO) << "SSD IO(r/w) = " << ssd_io_r_byte << "/" << ssd_io_w_byte
              << "(Byte)";

    gbp::warmup_mark().store(0);
    std::cout << "cost time:"
              << std::chrono::duration_cast<std::chrono::milliseconds>(end -
                                                                       begin)
                     .count()
              << "\n";
    Req::get().output();

    LOG(INFO) << "global counter 9 = " << gbp::get_counter_global(9);
    LOG(INFO) << "global counter 10 = " << gbp::get_counter_global(10);
    LOG(INFO) << "global counter 11 = " << gbp::get_counter_global(11);
    LOG(INFO) << "global counter 12 = " << gbp::get_counter_global(12);

    gbp::warmup_mark().store(1);
    gbp::DirectCache::CleanAllCache();
  }
  LOG(INFO) << " 19 = " << gbp::get_counter_global(19);
  LOG(INFO) << " 20 = " << gbp::get_counter_global(20);
  Req::get().LoggerStop();
  auto memory_usages =
      gbp::BufferPoolManager::GetGlobalInstance().GetMemoryUsage();

  LOG(INFO) << std::get<0>(memory_usages) << " | " << std::get<1>(memory_usages)
            << " | " << std::get<2>(memory_usages) << " | "
            << std::get<3>(memory_usages) << " | "
            << std::get<4>(memory_usages);
}