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

#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/graph_db/app/app_base.h"
#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/utils/app_utils.h"

namespace gs {

ReadTransaction GraphDBSession::GetReadTransaction() {
  uint32_t ts = db_.version_manager_.acquire_read_timestamp();
  return ReadTransaction(db_.graph_, db_.version_manager_, ts);
}

InsertTransaction GraphDBSession::GetInsertTransaction() {
  uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
  return InsertTransaction(db_.graph_, alloc_, logger_, db_.version_manager_,
                           ts);
}

SingleVertexInsertTransaction
GraphDBSession::GetSingleVertexInsertTransaction() {
  uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
  return SingleVertexInsertTransaction(db_.graph_, alloc_, logger_,
                                       db_.version_manager_, ts);
}

SingleEdgeInsertTransaction GraphDBSession::GetSingleEdgeInsertTransaction() {
  uint32_t ts = db_.version_manager_.acquire_insert_timestamp();
  return SingleEdgeInsertTransaction(db_.graph_, alloc_, logger_,
                                     db_.version_manager_, ts);
}

UpdateTransaction GraphDBSession::GetUpdateTransaction() {
  uint32_t ts = db_.version_manager_.acquire_update_timestamp();
  return UpdateTransaction(db_.graph_, alloc_, work_dir_, logger_,
                           db_.version_manager_, ts);
}

const MutablePropertyFragment& GraphDBSession::graph() const {
  return db_.graph();
}

MutablePropertyFragment& GraphDBSession::graph() { return db_.graph(); }

const Schema& GraphDBSession::schema() const { return db_.schema(); }

std::shared_ptr<ColumnBase> GraphDBSession::get_vertex_property_column(
    uint8_t label, const std::string& col_name) const {
  return db_.get_vertex_property_column(label, col_name);
}

std::shared_ptr<RefColumnBase> GraphDBSession::get_vertex_id_column(
    uint8_t label) const {
  return std::make_shared<TypedRefColumn<oid_t>>(
      db_.graph().lf_indexers_[label].get_keys(), StorageStrategy::kMem);
}

// #define likely(x) __builtin_expect(!!(x), 1)

std::vector<char> GraphDBSession::Eval(const std::string& input) {
  // auto ts1 = gbp::GetSystemTime();
  uint8_t type = input.back();
  const char* str_data = input.data();
  size_t str_len = input.size() - 1;

  std::vector<char> result_buffer;

  auto query_id_t = gbp::get_query_id().load();

  // assert((int) type == 31);
  // if (!((int) type == 7))
  //   return result_buffer;
  // if (gbp::get_query_id() != 38237)
  //   return result_buffer;
  // static size_t count = 0;
  // count++;

  // if (count > 3)
  //   return result_buffer;
  // // if ((int) type == 1)
  // //   assert(false);
  // LOG(INFO) << (int) type << " " << gbp::get_query_id().load() << " "
  //           << gbp::get_thread_id();
  static std::atomic<size_t> query_id = 0;
  gbp::get_counter_query().fetch_add(1);

  Decoder decoder(str_data, str_len);
  Encoder encoder(result_buffer);

  AppBase* app = nullptr;
  if (likely(apps_[type] != nullptr)) {
    app = apps_[type];
  } else {
    app_wrappers_[type] = db_.CreateApp(type, thread_id_);
    if (app_wrappers_[type].app() == NULL) {
      LOG(ERROR) << "[Query-" + std::to_string((int) type)
                 << "] is not registered...";
      return result_buffer;
    } else {
      apps_[type] = app_wrappers_[type].app();
      app = apps_[type];
    }
  }
#ifdef DEBUG_1
  gbp::get_counter(1) = 0;
  gbp::get_counter(2) = 0;
  gbp::get_counter(11) = 0;
  gbp::get_counter(12) = 0;
  size_t ts = gbp::GetSystemTime();
#endif
#ifdef PROFILE_QUERY_LATENCY
  size_t ts1 = gbp::GetSystemTime();
#endif
  // LOG(INFO) << "\n" << gbp::get_results_vec()[gbp::get_query_id().load()];
  // assert(false);
  // LOG(INFO) << "query id = " << query_id.load() << " | " << (int) type;
  if (app->Query(decoder, encoder)) {
    // LOG(INFO) << "result_buffer.size() = " << result_buffer.size();
    // LOG(INFO) << "\n"
    //           << std::string_view{result_buffer.data(),
    //           result_buffer.size()};
    // assert(false);
#ifdef DEBUG_1
    ts = gbp::GetSystemTime() - ts;
    LOG(INFO) << "profiling: [" << gbp::get_query_id().load() << "][" << ts
              << " | " << gbp::get_counter(1) << " | " << gbp::get_counter(2)
              << " | " << gbp::get_counter(11) << " | " << gbp::get_counter(12)
              << "]";
#endif
    constexpr bool store_query = false;
    constexpr bool check_result = false;

    if constexpr (store_query) {
      static const size_t max_query_num = 2100;
      size_t cur_query_id = query_id.fetch_add(1);
      static std::atomic<size_t> query_tofile_count = 0;

      if (cur_query_id < max_query_num) {
        std::lock_guard lock(gbp::get_log_lock());

        gbp::write_to_query_file(input);
        gbp::write_to_result_file({result_buffer.data(),
        result_buffer.size()});
        query_tofile_count.fetch_add(1);
        if (query_tofile_count % 1000 == 0) {
          gbp::write_to_query_file(input, true);
          gbp::write_to_result_file(
              {result_buffer.data(), result_buffer.size()}, true);
        }
        if (query_tofile_count % 1000000 == 0) {
          LOG(INFO) << query_tofile_count / 1000000 << "M";
        }
        if (query_tofile_count == max_query_num) {
          gbp::write_to_query_file(input, true);
          gbp::write_to_result_file(
              {result_buffer.data(), result_buffer.size()}, true);
          LOG(INFO) << "file content has flushed to the file";
        }
      }
    }
    // LOG(INFO) << std::string_view{result_buffer.data(),
    // result_buffer.size()};
    if constexpr (check_result) {
      if (gbp::get_results_vec()[gbp::get_query_id().load()].size() != 0 &&
          gbp::get_results_vec()[gbp::get_query_id().load()] !=
              std::string_view{result_buffer.data(), result_buffer.size()}) {
        LOG(INFO) << "\n" << gbp::get_results_vec()[gbp::get_query_id().load()];
        LOG(INFO) << "\n"
                  << gbp::get_results_vec()[gbp::get_query_id().load()].size();

        LOG(INFO) << "=========";
        LOG(INFO) << "\n"
                  << std::string_view{result_buffer.data(),
                                      result_buffer.size()};
        LOG(FATAL) << (int) type << " " << gbp::get_query_id().load();
      }
    }
    // auto ts2 = gbp::GetSystemTime();
    // gbp::get_thread_logfile()
    //     << ts2 << " " << ts1 << " " << (int) type << std::endl;

    return result_buffer;
  }

  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] retry - 1 / 3";
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  decoder.reset(str_data, str_len);
  result_buffer.clear();

  if (app->Query(decoder, encoder)) {
    return result_buffer;
  }

  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] retry - 2 / 3";
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  decoder.reset(str_data, str_len);
  result_buffer.clear();

  if (app->Query(decoder, encoder)) {
    return result_buffer;
  }

  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] retry - 3 / 3";
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  decoder.reset(str_data, str_len);
  result_buffer.clear();

  if (app->Query(decoder, encoder)) {
    return result_buffer;
  }
  LOG(INFO) << "[Query-" << (int) type << "][Thread-" << thread_id_
            << "] failed after 3 retries";

  result_buffer.clear();
  return result_buffer;
}  // namespace gs

#undef likely

void GraphDBSession::GetAppInfo(Encoder& result) { db_.GetAppInfo(result); }

int GraphDBSession::SessionId() const { return thread_id_; }

}  // namespace gs
