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
#include "flex/graphscope_bufferpool/include/logger.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

ReadTransaction::ReadTransaction(MutablePropertyFragment& graph,
                                 VersionManager& vm, timestamp_t timestamp)
    : graph_(graph), vm_(vm), timestamp_(timestamp) {}
ReadTransaction::~ReadTransaction() { release(); }

timestamp_t ReadTransaction::timestamp() const { return timestamp_; }

void ReadTransaction::Commit() { release(); }

void ReadTransaction::Abort() { release(); }

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

const Schema& ReadTransaction::schema() const { return graph_.schema(); }

void ReadTransaction::release() {
  if (timestamp_ != std::numeric_limits<timestamp_t>::max()) {
    vm_.release_read_timestamp();
    timestamp_ = std::numeric_limits<timestamp_t>::max();
  }
}

ReadTransaction::vertex_iterator::vertex_iterator(
    label_t label, vid_t cur, vid_t num, const MutablePropertyFragment& graph)
    : label_(label), cur_(cur), num_(num), graph_(graph) {
  assert(false);
}
ReadTransaction::vertex_iterator::~vertex_iterator() = default;

bool ReadTransaction::vertex_iterator::IsValid() const {
  assert(false);
  return cur_ < num_;
}
void ReadTransaction::vertex_iterator::Next() {
  assert(false);
  ++cur_;
}
void ReadTransaction::vertex_iterator::Goto(vid_t target) {
  assert(false);
  cur_ = std::min(target, num_);
}

oid_t ReadTransaction::vertex_iterator::GetId() const {
  assert(false);
  return graph_.get_oid(label_, cur_);
}
vid_t ReadTransaction::vertex_iterator::GetIndex() const {
  assert(false);
  return cur_;
}

gbp::BufferBlock ReadTransaction::vertex_iterator::GetField(int col_id) const {
  assert(false);
  return graph_.get_vertex_table(label_).get_column_by_id(col_id)->get(cur_);
}

int ReadTransaction::vertex_iterator::FieldNum() const {
  assert(false);
  return graph_.get_vertex_table(label_).col_num();
}

ReadTransaction::edge_iterator::edge_iterator(
    label_t neighbor_label, label_t edge_label,
    std::shared_ptr<MutableCsrConstEdgeIterBase> iter)
    : neighbor_label_(neighbor_label),
      edge_label_(edge_label),
      iter_(std::move(iter)) {}
ReadTransaction::edge_iterator::~edge_iterator() = default;

const void* ReadTransaction::edge_iterator::GetData() const {
  assert(false);
  return iter_->get_data();
}

bool ReadTransaction::edge_iterator::IsValid() const {
  assert(false);
  return iter_->is_valid();
}

void ReadTransaction::edge_iterator::Next() {
  assert(false);
  iter_->next();
}

vid_t ReadTransaction::edge_iterator::GetNeighbor() const {
  assert(false);
  return iter_->get_neighbor();
}

label_t ReadTransaction::edge_iterator::GetNeighborLabel() const {
  assert(false);
  return neighbor_label_;
}

label_t ReadTransaction::edge_iterator::GetEdgeLabel() const {
  assert(false);
  return edge_label_;
}

ReadTransaction::vertex_iterator ReadTransaction::GetVertexIterator(
    label_t label) const {
  assert(false);
  return {label, 0, graph_.vertex_num(label), graph_};
}

ReadTransaction::vertex_iterator ReadTransaction::FindVertex(label_t label,
                                                             oid_t id) const {
  assert(false);
  vid_t lid;
  if (graph_.get_lid(label, id, lid)) {
    return {label, lid, graph_.vertex_num(label), graph_};
  } else {
    return {label, graph_.vertex_num(label), graph_.vertex_num(label), graph_};
  }
}

ReadTransaction::edge_iterator ReadTransaction::GetOutEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  assert(false);
  return {neighnor_label, edge_label,
          graph_.get_outgoing_edges(label, u, neighnor_label, edge_label)};
}

ReadTransaction::edge_iterator ReadTransaction::GetInEdgeIterator(
    label_t label, vid_t u, label_t neighnor_label, label_t edge_label) const {
  assert(false);
  return {neighnor_label, edge_label,
          graph_.get_incoming_edges(label, u, neighnor_label, edge_label)};
}

std::vector<oid_t> ReadTransaction::BatchGetVertexIds(label_t label,
                                       const std::vector<vid_t>& indices) const{
  std::vector<oid_t> oids(indices.size());
  for (size_t i = 0; i < indices.size(); ++i) {
    oids[i] = graph_.get_oid(label, indices[i]);
  }
  return oids;
}

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

template <typename EDATA_T>
std::vector<AdjListView<EDATA_T>> ReadTransaction::BatchGetOutgoingEdges(
    label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
    label_t edge_label) const {
  std::vector<AdjListView<EDATA_T>> results;
  results.reserve(vids.size());
  for (auto v : vids) {
    results.push_back(
        GetOutgoingEdges<EDATA_T>(v_label, v, neighbor_label, edge_label));
  }
  return results;
}

template <typename EDATA_T>
std::vector<AdjListView<EDATA_T>> ReadTransaction::BatchGetIncomingEdges(
    label_t v_label, const std::vector<vid_t>& vids, label_t neighbor_label,
    label_t edge_label) const {
  std::vector<AdjListView<EDATA_T>> results;
  results.reserve(vids.size());
  for (auto v : vids) {
    results.push_back(
        GetIncomingEdges<EDATA_T>(v_label, v, neighbor_label, edge_label));
  }
  return results;
}
template <typename EDATA_T>
std::vector<std::vector<vid_t>> ReadTransaction::BatchGetVidsNeighbors(//这里需要区分out和in
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const {
  std::vector<std::vector<vid_t>> ret;
  ret.resize(vids.size());
  unsigned int edge_label_with_direction;
  if(is_out){
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    v_label, neighbor_label, edge_label, is_out);
  }else {
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    neighbor_label, v_label,  edge_label, is_out);
  }
  auto edge_handle=graph_.get_vertices(v_label).getEdgeHandle(edge_label_with_direction);
  for(int i=0;i<vids.size();i++){
    size_t edge_size;
    auto item_t=edge_handle.getEdges(vids[i], edge_size);
    ret[i].reserve(edge_size);
    for(int j=0;j<edge_size;j++){
      auto nbr=gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item_t, j).neighbor;
      ret[i].push_back(nbr);
    }
  }
  return ret;
}

template <typename EDATA_T>
std::vector<vid_t> ReadTransaction::BatchGetVidsNeighborsWithIndex(//这里需要区分out和in
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    std::vector<std::pair<int,int>>& neighbors_index,
    bool is_out) const {
  std::vector<vid_t> ret;
  ret.resize(vids.size());
  unsigned int edge_label_with_direction;
  if(is_out){
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    v_label, neighbor_label, edge_label, is_out);
  }else {
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    neighbor_label, v_label,  edge_label, is_out);
  }
  auto edge_handle=graph_.get_vertices(v_label).getEdgeHandle(edge_label_with_direction);
  int size=0;
  for(int i=0;i<vids.size();i++){
    size_t edge_size;
    auto item_t=edge_handle.getEdges(vids[i], edge_size);
    size+=edge_size;
  }
  ret.reserve(size);
  for(int i=0;i<vids.size();i++){
    neighbors_index.push_back(std::make_pair(0,0));
    neighbors_index[i].first=ret.size();
    size_t edge_size;
    auto item_t=edge_handle.getEdges(vids[i], edge_size);
    for(int j=0;j<edge_size;j++){
      auto nbr=gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item_t, j).neighbor;
      ret.push_back(nbr);
    }
    neighbors_index[i].second=ret.size();
  }
  return ret;
}

template <typename EDATA_T>
std::vector<std::vector<std::pair<vid_t,timestamp_t>>> ReadTransaction::BatchGetVidsNeighborsWithTimestamp(//这里需要区分out和in
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const {
  std::vector<std::vector<std::pair<vid_t,timestamp_t>>> ret;
  ret.resize(vids.size());
  unsigned int edge_label_with_direction;
  if(is_out){
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    v_label, neighbor_label, edge_label, is_out);
  }else {
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    neighbor_label, v_label,  edge_label, is_out);
  }
  auto edge_handle=graph_.get_vertices(v_label).getEdgeHandle(edge_label_with_direction);
  for(int i=0;i<vids.size();i++){
    size_t edge_size;
    auto item_t=edge_handle.getEdges(vids[i], edge_size);
    ret[i].reserve(edge_size);
    for(int j=0;j<edge_size;j++){
      auto nbr=gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item_t, j).neighbor;
      auto timestamp=gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item_t, j).timestamp.load();
      ret[i].push_back(std::make_pair(nbr,timestamp));
    }
  }
  return ret;
}

std::vector<std::vector<gbp::BufferBlock>> ReadTransaction::BatchGetVertexPropsFromVids(
    label_t label_id, const std::vector<vid_t>& vids,
    const std::vector<cgraph::PropertyHandle>& prop_handles) const {
  std::vector<std::vector<gbp::BufferBlock>> results;
  results.reserve(prop_handles.size());
  for(int i=0;i<prop_handles.size();i++){
    results.push_back(std::vector<gbp::BufferBlock>());
    results[i].reserve(vids.size());
    for(auto vid:vids){
      results[i].push_back(prop_handles[i].getProperty(vid));
    }
  }
  return results;
}

template std::vector<std::vector<std::pair<vid_t,timestamp_t>>> ReadTransaction::BatchGetVidsNeighborsWithTimestamp<grape::EmptyType>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const;

template std::vector<std::vector<std::pair<vid_t,timestamp_t>>> ReadTransaction::BatchGetVidsNeighborsWithTimestamp<Date>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const;

template std::vector<std::vector<std::pair<vid_t,timestamp_t>>> ReadTransaction::BatchGetVidsNeighborsWithTimestamp<int>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const;

template std::vector<std::vector<vid_t>> ReadTransaction::BatchGetVidsNeighbors<int>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const;

template std::vector<std::vector<vid_t>> ReadTransaction::BatchGetVidsNeighbors<grape::EmptyType>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const;

template std::vector<std::vector<vid_t>> ReadTransaction::BatchGetVidsNeighbors<Date>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    bool is_out) const;

template std::vector<vid_t> ReadTransaction::BatchGetVidsNeighborsWithIndex<int>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    std::vector<std::pair<int,int>>& neighbors_index,
    bool is_out) const;

template std::vector<vid_t> ReadTransaction::BatchGetVidsNeighborsWithIndex<grape::EmptyType>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    std::vector<std::pair<int,int>>& neighbors_index,
    bool is_out) const;

template std::vector<vid_t> ReadTransaction::BatchGetVidsNeighborsWithIndex<Date>(
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    std::vector<std::pair<int,int>>& neighbors_index,
    bool is_out) const;


template std::vector<AdjListView<Date>>
ReadTransaction::BatchGetOutgoingEdges<Date>(label_t, const std::vector<vid_t>&,
                                             label_t, label_t) const;

template std::vector<AdjListView<Date>>
ReadTransaction::BatchGetIncomingEdges<Date>(label_t, const std::vector<vid_t>&,
                                             label_t, label_t) const;

template std::vector<AdjListView<grape::EmptyType>>
ReadTransaction::BatchGetIncomingEdges<grape::EmptyType>(
    label_t, const std::vector<vid_t>&, label_t, label_t) const;

template <typename EDATA_T>
std::vector<std::vector<std::pair<vid_t, EDATA_T>>> ReadTransaction::BatchGetEdgePropsFromSrcVids(
      const label_t& v_label,
      const label_t& neighbor_label, 
      const label_t& edge_label,
      const std::vector<vid_t>& vids,
      bool is_out) const{

  std::vector<std::vector<std::pair<vid_t, EDATA_T>>> ret;
  ret.resize(vids.size());
    unsigned int edge_label_with_direction;
  if(is_out){
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    v_label, neighbor_label, edge_label, is_out);
  }else {
    edge_label_with_direction=graph_.schema().generate_edge_label_with_direction(
    neighbor_label, v_label,  edge_label, is_out);
  }
  auto edge_handle=graph_.get_vertices(v_label).getEdgeHandle(edge_label_with_direction);
  for(int i=0;i<vids.size();i++){
    size_t edge_size;
    auto item_t=edge_handle.getEdges(vids[i], edge_size);
    ret[i].reserve(edge_size);
    for(int j=0;j<edge_size;j++){
      auto item_neighbor=gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item_t, j);
      auto nbr=item_neighbor.neighbor;
      auto edge_data=item_neighbor.data;
      ret[i].push_back(std::make_pair(nbr, edge_data));
    }
  }
  return ret;
}

// 添加常用类型的模板实例化
template std::vector<std::vector<std::pair<vid_t, Date>>> 
ReadTransaction::BatchGetEdgePropsFromSrcVids<Date>(
    const label_t&, const label_t&, const label_t&,
    const std::vector<vid_t>&, bool) const;

template std::vector<std::vector<std::pair<vid_t, int>>> 
ReadTransaction::BatchGetEdgePropsFromSrcVids<int>(
    const label_t&, const label_t&, const label_t&,
    const std::vector<vid_t>&, bool) const;

template std::vector<std::vector<std::pair<vid_t, grape::EmptyType>>> 
ReadTransaction::BatchGetEdgePropsFromSrcVids<grape::EmptyType>(
    const label_t&, const label_t&, const label_t&,
    const std::vector<vid_t>&, bool) const;

}  // namespace gs
