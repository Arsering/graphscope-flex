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

#include "flex/engines/graph_db/database/read_transaction.h"
#include "flex/engines/graph_db/database/version_manager.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

namespace gs {

ReadTransaction::ReadTransaction(const MutablePropertyFragment& graph,
                                 VersionManager& vm, timestamp_t timestamp)
    : graph_(graph),
      vm_(vm),
      timestamp_(timestamp),
      buffer_pool_manager_(&gbp::BufferPoolManager::GetGlobalInstance()) {}
ReadTransaction::~ReadTransaction() { release(); }

timestamp_t ReadTransaction::timestamp() const { return timestamp_; }

void ReadTransaction::Commit() { release(); }

void ReadTransaction::Abort() { release(); }

ReadTransaction::vertex_iterator::vertex_iterator(
    label_t label, vid_t cur, vid_t num, const MutablePropertyFragment& graph)
    : label_(label), cur_(cur), num_(num), graph_(graph) {}
ReadTransaction::vertex_iterator::~vertex_iterator() = default;

bool ReadTransaction::vertex_iterator::IsValid() const { return cur_ < num_; }
void ReadTransaction::vertex_iterator::Next() { ++cur_; }
void ReadTransaction::vertex_iterator::Goto(vid_t target) {
  cur_ = std::min(target, num_);
}

oid_t ReadTransaction::vertex_iterator::GetId() const {
  return graph_.get_oid(label_, cur_);
}
vid_t ReadTransaction::vertex_iterator::GetIndex() const { return cur_; }

#if OV
Any ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}
#else
gbp::BufferBlock ReadTransaction::vertex_iterator::GetField(int col_id) const {
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}
#endif

int ReadTransaction::vertex_iterator::FieldNum() const {
  return graph_.get_vertex_table(label_).col_num();
}

ReadTransaction::edge_iterator::edge_iterator(
    label_t neighbor_label, label_t edge_label,
    std::shared_ptr<MutableCsrConstEdgeIterBase> iter)
    : neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      iter_(std::move(iter)) {}
ReadTransaction::edge_iterator::~edge_iterator() = default;

#if OV
Any ReadTransaction::edge_iterator::GetData() const {
  return iter_->get_data();
}
#else
const void* ReadTransaction::edge_iterator::GetData() const {
  return iter_->get_data();
}
#endif

bool ReadTransaction::edge_iterator::IsValid() const {
  return iter_->is_valid();
}

void ReadTransaction::edge_iterator::Next() { iter_->next(); }

vid_t ReadTransaction::edge_iterator::GetNeighbor() const {
  return iter_->get_neighbor();
}

label_t ReadTransaction::edge_iterator::GetNeighborLabel() const {
  return neighbor_label_;
}

label_t ReadTransaction::edge_iterator::GetEdgeLabel() const {
  return edge_label_;
}

ReadTransaction::vertex_iterator ReadTransaction::GetVertexIterator(
    label_t label) const {
  return {label, 0, graph_.vertex_num(label), graph_};
}

ReadTransaction::vertex_iterator ReadTransaction::FindVertex(label_t label,
                                                             oid_t id) const {
  vid_t lid;
  if (graph_.get_lid(label, id, lid)) {
    return {label, lid, graph_.vertex_num(label), graph_};
  } else {
    return {label, graph_.vertex_num(label), graph_.vertex_num(label), graph_};
  }
}

bool ReadTransaction::GetVertexIndex(label_t label, oid_t id,
                                     vid_t& index) const {
  return graph_.get_lid(label, id, index);
}

vid_t ReadTransaction::GetVertexNum(label_t label) const {
  return graph_.vertex_num(label);
}

oid_t ReadTransaction::GetVertexId(label_t label, vid_t index) const {
  return graph_.get_oid(label, index);
}

ReadTransaction::edge_iterator ReadTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_outgoing_edges(label, u, neighnor_label, edge_label)};
}

ReadTransaction::edge_iterator ReadTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  return {neighnor_label, edge_label,
          graph_.get_incoming_edges(label, u, neighnor_label, edge_label)};
}

const Schema& ReadTransaction::schema() const { return graph_.schema(); }

void ReadTransaction::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    vm_.release_read_timestamp();
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

// ========================== batching 接口 ==========================

std::vector<oid_t> ReadTransaction::BatchGetVertexIds(
    label_t label, const std::vector<vid_t>& indices) const {
  std::vector<oid_t> oids(indices.size());
  std::vector<gbp::batch_request_type> requests(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    requests[i] = graph_.get_oid_batch(label, indices[i]);
  }
  auto blocks = buffer_pool_manager_->GetBlockBatch(requests);
  for (size_t i = 0; i < indices.size(); ++i) {
    oids[i] = gbp::BufferBlock::Ref<oid_t>(blocks[i]);
  }
  return oids;
}

// TODO: 该函数暂时不实现，其中包含查找过程，实现比较复杂
std::pair<std::vector<vid_t>, std::vector<bool>>
ReadTransaction::BatchGetVertexIndices(label_t label,
                                       const std::vector<oid_t>& oids) const {
  std::vector<vid_t> indices(oids.size());
  std::vector<bool> exists(oids.size());
  for (size_t i = 0; i < oids.size(); ++i) {
    exists[i] = graph_.get_lid(label, oids[i], indices[i]);
  }
  return {indices, exists};
}

// TODO:
// 实现思路：修改GetOutgoingEdges和AdjListView，其中AdjListView直接给构造函数传入一个bufferblock.
template <typename EDATA_T>
std::vector<AdjListView<EDATA_T>> ReadTransaction::BatchGetOutgoingEdges(
    label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
    label_t edge_label) const {
  std::vector<gbp::batch_request_type> requests;
  std::vector<AdjListView<EDATA_T>> results;
  // results.reserve(vids.size());
  // 获取所有邻接列表
  for (auto v : vids) {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    requests.emplace_back(csr->get_edgelist_batch(v));
  }
  auto adjlist_blocks = buffer_pool_manager_->GetBlockBatch(requests);

  // 获取所有边列表
  requests.clear();
  for (size_t i = 0; i < adjlist_blocks.size(); ++i) {
    auto& adj_list =
        gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adjlist_blocks[i]);
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    requests.emplace_back(
        csr->get_edges_batch(adj_list.start_idx_, adj_list.size_));
  }
  auto edges_blocks = buffer_pool_manager_->GetBlockBatch(requests);

  // 生成最后结果
  for (size_t i = 0; i < edges_blocks.size(); ++i) {
    results.emplace_back(
        edges_blocks[i],
        gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adjlist_blocks[i]).size_,
        timestamp_);
  }

  return results;
}

template <typename EDATA_T>
std::vector<AdjListView<EDATA_T>> ReadTransaction::BatchGetIncomingEdges(
    label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
    label_t edge_label) const {
  std::vector<gbp::batch_request_type> requests;
  std::vector<AdjListView<EDATA_T>> results;
  // results.reserve(vids.size());
  // 获取所有邻接列表
  for (auto v : vids) {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    requests.emplace_back(csr->get_edgelist_batch(v));
  }
  auto adjlist_blocks = buffer_pool_manager_->GetBlockBatch(requests);
  requests.clear();

  // 获取所有边列表
  for (size_t i = 0; i < adjlist_blocks.size(); ++i) {
    auto& adj_list =
        gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adjlist_blocks[i]);
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    requests.emplace_back(
        csr->get_edges_batch(adj_list.start_idx_, adj_list.size_));
  }
  auto edges_blocks = buffer_pool_manager_->GetBlockBatch(requests);

  // 生成最后结果
  for (size_t i = 0; i < edges_blocks.size(); ++i) {
    results.emplace_back(
        edges_blocks[i],
        gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adjlist_blocks[i]).size_,
        timestamp_);
  }

  return results;
}

template <typename EDATA_T>
std::vector<gbp::BufferBlock> ReadTransaction::BatchGetOutgoingSingleEdges(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids) const {
  std::vector<gbp::batch_request_type> requests;

  for (auto v : vids) {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    requests.emplace_back(csr->get_edges_batch(v));
  }
  return buffer_pool_manager_->GetBlockBatch(requests);
}

template <typename EDATA_T>
std::vector<gbp::BufferBlock> ReadTransaction::BatchGetIncomingSingleEdges(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids) const {
  std::vector<gbp::batch_request_type> requests;

  for (auto v : vids) {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    requests.emplace_back(csr->get_edges_batch(v));
  }
  return buffer_pool_manager_->GetBlockBatch(requests);
}

std::vector<std::vector<gbp::BufferBlock>>
ReadTransaction::BatchGetVertexPropsFromVids(
    label_t label_id, const std::vector<vid_t>& vids,
    const std::vector<std::string>& prop_names) const {
  std::vector<gbp::batch_request_type> requests;
  std::vector<std::pair<uint32_t, uint32_t>> string_column_idxs;
  auto& table = graph_.get_vertex_table(label_id);

  // 获取所有列
  std::vector<std::shared_ptr<ColumnBase>> columns;
  columns.reserve(prop_names.size());
  for (const auto& prop_name : prop_names) {
    auto column = table.get_column(prop_name);
    CHECK(column != nullptr) << "Column " << prop_name << " not found";
    columns.push_back(column);
  }

  for (size_t column_idx = 0; column_idx < columns.size(); column_idx++) {
    auto column = columns[column_idx];
    switch (column->type()) {
    case PropertyType::kString: {
      auto string_column = std::dynamic_pointer_cast<StringColumn>(column);
      CHECK(string_column != nullptr);
      for (auto vid : vids) {
        requests.emplace_back(string_column->get_stringview_batch(vid));
        string_column_idxs.emplace_back(requests.size() - 1, column_idx);
      }
      break;
    }
    case PropertyType::kDate: {
      auto date_column = std::dynamic_pointer_cast<TypedColumn<Date>>(column);
      CHECK(date_column != nullptr);
      for (auto vid : vids) {
        requests.emplace_back(date_column->get_batch(vid));
      }
      break;
    }
    case PropertyType::kInt32: {
      auto int_column = std::dynamic_pointer_cast<TypedColumn<int>>(column);
      CHECK(int_column != nullptr);
      for (auto vid : vids) {
        requests.emplace_back(int_column->get_batch(vid));
      }
      break;
    }
    case PropertyType::kInt64: {
      auto int64_column =
          std::dynamic_pointer_cast<TypedColumn<int64_t>>(column);
      CHECK(int64_column != nullptr);
      for (auto vid : vids) {
        requests.emplace_back(int64_column->get_batch(vid));
      }
      break;
    }
    default: {
      LOG(FATAL) << "Unsupported property type: "
                 << static_cast<int>(column->type());
    }
    }
  }

  auto results = buffer_pool_manager_->GetBlockBatch(requests);

  // 对string需要特别处理
  {
    requests.clear();
    for (size_t i = 0; i < string_column_idxs.size(); ++i) {
      auto& item = gbp::BufferBlock::Ref<gs::string_item>(
          results[string_column_idxs[i].first]);
      requests.emplace_back(std::dynamic_pointer_cast<StringColumn>(
                                columns[string_column_idxs[i].second])
                                ->get_batch(item.offset, item.length));
    }

    auto string_blocks = buffer_pool_manager_->GetBlockBatch(requests);
    for (size_t i = 0; i < string_column_idxs.size(); ++i) {
      results[string_column_idxs[i].first] = string_blocks[i];
    }
  }
  std::vector<std::vector<gbp::BufferBlock>> results_vec;
  uint32_t result_idx = 0;
  results_vec.resize(columns.size());
  for (size_t column_idx = 0; column_idx < columns.size(); column_idx++) {
    results_vec[column_idx].resize(vids.size());
    for (size_t vid_idx = 0; vid_idx < vids.size(); vid_idx++) {
      results_vec[column_idx][vid_idx] = results[result_idx++];
    }
  }

  return results_vec;
}

// std::vector<std::vector<gbp::BufferBlock>>
// ReadTransaction::BatchGetVertexPropsFromVids(
//     label_t label_id, const std::vector<vid_t>& vids,
//     const std::vector<ColumnBase*>& prop_columns) const {
//   std::vector<std::vector<gbp::BufferBlock>> results;
//   results.reserve(prop_columns.size());
//   assert(false);
//   // for (int i = 0; i < prop_columns.size(); i++) {
//   //   results.push_back(std::vector<gbp::BufferBlock>());
//   //   results[i].reserve(vids.size());
//   //   for (auto vid : vids) {
//   //     // results[i].push_back(prop_handles[i].getProperty(vid));
//   //     auto item_t_index = prop_columns[i]->getPropertyIndex(vid);
//   //     results[i].push_back(
//   //         gbp::BufferPoolManager::GetGlobalInstance().GetBlockSync(
//   //             item_t_index.file_offset, item_t_index.buf_size,
//   //             item_t_index.fd_gbp));
//   //   }
//   // }
//   return results;
// }

template std::vector<AdjListView<Date>>
ReadTransaction::BatchGetOutgoingEdges<Date>(label_t, const std::vector<vid_t>&,
                                             label_t, label_t) const;

template std::vector<AdjListView<Date>>
ReadTransaction::BatchGetIncomingEdges<Date>(label_t, const std::vector<vid_t>&,
                                             label_t, label_t) const;

template std::vector<AdjListView<grape::EmptyType>>
ReadTransaction::BatchGetIncomingEdges<grape::EmptyType>(
    label_t, const std::vector<vid_t>&, label_t, label_t) const;
// ========================== batching 接口 ==========================

}  // namespace gs
