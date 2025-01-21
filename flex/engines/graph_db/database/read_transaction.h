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

#ifndef GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
#define GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_

#include <cstddef>
#include <limits>
#include <utility>

#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/mutable_csr_cgraph.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"
#include "flex/storages/rt_mutable_graph/vertex.h"

namespace gs {

class MutablePropertyFragment;
class VersionManager;

template <typename EDATA_T>
class AdjListView {
 public:
  AdjListView(gbp::BufferBlock item, timestamp_t timestamp, int size)
      : edges_(item), timestamp_(timestamp), current_index_(0), size_(size) {
    // LOG(INFO) << "init adjlistview bufferblock size=" << edges_.GetSize()<<",
    // size_="<<size_;
    while (current_index_ < size_ &&
           gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(edges_, current_index_)
                   .timestamp.load() > timestamp_) {
      current_index_++;
    }
  }

  FORCE_INLINE vid_t get_neighbor() {
#if ASSERT_ENABLE
    assert(current_index_ < size_);
#endif

    return gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(edges_, current_index_)
        .neighbor;
  }

  FORCE_INLINE const void* get_data() {
#if ASSERT_ENABLE
    assert(current_index_ < size_);
#endif

    return static_cast<const void*>(
        &(gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(edges_, current_index_)
              .data));
  }

  FORCE_INLINE timestamp_t get_timestamp() {
#if ASSERT_ENABLE
    assert(current_index_ < size_);
#endif
    return gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(edges_, current_index_)
        .timestamp.load();
  }

  FORCE_INLINE void next() {
    current_index_++;
    while (current_index_ < size_ &&
           gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(edges_, current_index_)
                   .timestamp.load() > timestamp_) {
      current_index_++;
    }
  }

  FORCE_INLINE bool is_valid() {
    if (current_index_ < size_) {
      auto tmp =
          gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(edges_, current_index_);
      return tmp.timestamp.load() <= timestamp_;
    }
    return false;
  }

  int estimated_degree() const { return size_; }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  gbp::BufferBlock edges_;
  int current_index_;
  size_t size_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class GraphView {
 public:
  GraphView(label_t vertex_label, unsigned int edge_label_with_direction,
            timestamp_t timestamp, MutablePropertyFragment& graph)
      : timestamp_(timestamp),
        edge_handle_(graph.get_vertices(vertex_label)
                         .getEdgeHandle(edge_label_with_direction)) {}

  AdjListView<EDATA_T> get_edges(vid_t v) {
    size_t edge_size;
    auto item_t = edge_handle_.getEdges(v, edge_size);
    return AdjListView<EDATA_T>(item_t, timestamp_, edge_size);
  }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  timestamp_t timestamp_;
  cgraph::EdgeHandle edge_handle_;
};

template <typename EDATA_T>
class SingleGraphView {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  SingleGraphView(label_t vertex_label, unsigned int edge_label_with_direction,
                  timestamp_t timestamp, MutablePropertyFragment& graph)
      : timestamp_(timestamp),
        edge_handle_(graph.get_vertices(vertex_label)
                         .getEdgeHandle(edge_label_with_direction)) {}

  FORCE_INLINE bool exist(vid_t v) const {
    size_t edge_size;
    auto item = edge_handle_.getEdges(v, edge_size);
    return (gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_);
  }

  FORCE_INLINE const gbp::BufferBlock exist(vid_t v, bool& exist) const {
    size_t edge_size;
    auto item = edge_handle_.getEdges(v, edge_size);
    exist = gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
    return item;
  }
  FORCE_INLINE bool exist1(gbp::BufferBlock& item) const {
    return gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
  }
  FORCE_INLINE const gbp::BufferBlock get_edge(vid_t v) const {
    size_t edge_size;

    return edge_handle_.getEdges(v, edge_size);
  }

  FORCE_INLINE timestamp_t timestamp() const { return timestamp_; }

 private:
  // label_t vertex_label_;
  // unsigned int edge_label_with_direction_;
  timestamp_t timestamp_;
  cgraph::EdgeHandle edge_handle_;
  // MutablePropertyFragment& graph_;
};

class ReadTransaction {
 public:
  ReadTransaction(MutablePropertyFragment& graph, VersionManager& vm,
                  timestamp_t timestamp);
  ~ReadTransaction();

  timestamp_t timestamp() const;

  void Commit();

  void Abort();

  class vertex_iterator {
   public:
    vertex_iterator(label_t label, vid_t cur, vid_t num,
                    const MutablePropertyFragment& graph);
    ~vertex_iterator();

    bool IsValid() const;
    void Next();
    void Goto(vid_t target);

    oid_t GetId() const;
    vid_t GetIndex() const;
    gbp::BufferBlock GetField(int col_id) const;
    int FieldNum() const;

   private:
    label_t label_;
    vid_t cur_;
    vid_t num_;
    const MutablePropertyFragment& graph_;
  };

  class edge_iterator {
   public:
    edge_iterator(label_t neighbor_label, label_t edge_label,
                  std::shared_ptr<MutableCsrConstEdgeIterBase> iter);
    ~edge_iterator();

    const void* GetData() const;

    bool IsValid() const;

    void Next();

    vid_t GetNeighbor() const;

    label_t GetNeighborLabel() const;

    label_t GetEdgeLabel() const;

   private:
    label_t neighbor_label_;
    label_t edge_label_;

    std::shared_ptr<MutableCsrConstEdgeIterBase> iter_;
  };

  vertex_iterator GetVertexIterator(label_t label) const;

  vertex_iterator FindVertex(label_t label, oid_t id) const;

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighnor_label,
                                   label_t edge_label) const;

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighnor_label,
                                  label_t edge_label) const;

  bool GetVertexIndex(label_t label, oid_t id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  oid_t GetVertexId(label_t label, vid_t index) const;

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetOutgoingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto edge_label_id_with_direction =
        graph_.schema().generate_edge_label_with_direction(
            v_label, neighbor_label, edge_label, true);
    size_t edge_size;
    auto item_t = graph_.get_vertices(v_label).ReadEdges(
        v, edge_label_id_with_direction, edge_size);
    return AdjListView<EDATA_T>(item_t, timestamp_, edge_size);
  }

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetIncomingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto edge_label_id_with_direction =
        graph_.schema().generate_edge_label_with_direction(
            neighbor_label, v_label, edge_label, false);
    size_t edge_size;
    auto item_t = graph_.get_vertices(v_label).ReadEdges(
        v, edge_label_id_with_direction, edge_size);
    return AdjListView<EDATA_T>(item_t, timestamp_, edge_size);
  }

  const Schema& schema() const;

  template <typename EDATA_T>
  GraphView<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto edge_label_id_with_direction =
        graph_.schema().generate_edge_label_with_direction(
            v_label, neighbor_label, edge_label, true);
    return GraphView<EDATA_T>(v_label, edge_label_id_with_direction, timestamp_,
                              graph_);
  }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto edge_label_id_with_direction =
        graph_.schema().generate_edge_label_with_direction(
            neighbor_label, v_label, edge_label, false);
    return GraphView<EDATA_T>(v_label, edge_label_id_with_direction, timestamp_,
                              graph_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetOutgoingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto edge_label_id_with_direction =
        graph_.schema().generate_edge_label_with_direction(
            v_label, neighbor_label, edge_label, true);
    return SingleGraphView<EDATA_T>(v_label, edge_label_id_with_direction,
                                    timestamp_, graph_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetIncomingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto edge_label_id_with_direction =
        graph_.schema().generate_edge_label_with_direction(
            neighbor_label, v_label, edge_label, false);
    return SingleGraphView<EDATA_T>(v_label, edge_label_id_with_direction,
                                    timestamp_, graph_);
  }

  gbp::BufferBlock GetVertexProp(label_t label, vid_t v,
                                 std::string property_name) {
    auto property_id = graph_.schema().get_property_id(label, property_name);
    return graph_.get_vertices(label).ReadColumn(v, property_id);
  }

  gbp::BufferBlock GetVertexProp(label_t label, vid_t v, int property_id) {
    return graph_.get_vertices(label).ReadColumn(v, property_id);
  }

    /*
  批量获取顶点ID
  @param label: 顶点标签
  @param indices: 顶点索引组成的vector
  @return: 顶点ID组成的vector
  */
  std::vector<oid_t> BatchGetVertexIds(label_t label,
                                       const std::vector<vid_t>& indices) const;

  /*
  批量查找顶点索引
  @param label: 顶点标签
  @param oids: 顶点ID组成的vector
  @return: 顶点索引组成的vector和是否存在的vector
  */
  std::pair<std::vector<vid_t>, std::vector<bool>> BatchGetVertexIndices(
      label_t label, const std::vector<oid_t>& oids) const;


  /*
  批量获取顶点属性
  @param label_id: 顶点标签
  @param vids: 顶点ID组成的vector
  @param prop_names: 属性名称组成的vector
  @return: 顶点属性组成的vector，先是属性后是顶点
  */
  std::vector<std::vector<gbp::BufferBlock>> BatchGetVertexPropsFromVids(
      label_t label_id, const std::vector<vid_t>& vids,
      const std::vector<cgraph::PropertyHandle>& prop_handles) const;

  /* 获取边指向的邻居
  @param src_label_id: 源顶点标签
  @param dst_label_id: 目标顶点标签
  @param edge_label_id: 边标签
  @param vids: 顶点ID组成的vector
  @param is_out: 方向
  @return: 邻居列表
  */
  template <typename EDATA_T>
  std::vector<std::vector<vid_t>> BatchGetVidsNeighbors(
      const label_t& src_label_id, const label_t& dst_label_id,
      const label_t& edge_label_id, const std::vector<vid_t>& vids,
      bool is_out) const;
  template <typename EDATA_T>
  std::vector<vid_t> BatchGetVidsNeighborsWithIndex(//这里需要区分out和in
    const label_t& v_label, const label_t& neighbor_label,
    const label_t& edge_label, const std::vector<vid_t>& vids,
    std::vector<std::pair<int,int>>& neighbors_index,
    bool is_out) const;
  template <typename EDATA_T>
  std::vector<std::vector<std::pair<vid_t,timestamp_t>>> BatchGetVidsNeighborsWithTimestamp(
      const label_t& src_label_id, const label_t& dst_label_id,
      const label_t& edge_label_id, const std::vector<vid_t>& vids,
      bool is_out) const;
  template<typename EDATA_T>
  std::vector<vid_t> BatchGetVidNeighbors(
      vid_t v, size_t edge_size,cgraph::EdgeHandle edge_handle) const{
        std::vector<vid_t> ret;
        auto item_t=edge_handle.getEdges(v, edge_size);
        ret.reserve(edge_size);
        for(int i=0;i<edge_size;i++){
          auto item_neighbor=gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item_t, i);
          auto nbr=item_neighbor.neighbor;
          ret.push_back(nbr);
        }
        return ret;
  }

  /* 批量获取边属性
  @param v_label: 顶点标签
  @param neighbor_label: 邻居标签
  @param edge_label: 边标签
  @param vids: 顶点ID组成的vector
  @param is_out: 方向
  @return: 边属性组成的vector
  */
  template <typename EDATA_T>
  std::vector<std::vector<std::pair<vid_t, EDATA_T>>> BatchGetEdgePropsFromSrcVids(
      const label_t& v_label,
      const label_t& neighbor_label, 
      const label_t& edge_label,
      const std::vector<vid_t>& vids,
      bool is_out) const;

  template<typename EDATA_T>
  bool check_edge_exist(gbp::BufferBlock& item) const{
    return gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item).timestamp.load() <= timestamp_;
  }

  template<typename EDATA_T>
  bool check_edge_exist(AdjListView<EDATA_T>& item,size_t index=0) const{
    return gbp::BufferBlock::Ref<MutableNbr<EDATA_T>>(item.get_neighbor()).timestamp.load() <= timestamp_;
  }

  bool check_edge_exist(std::vector<std::pair<vid_t,timestamp_t>>& item,size_t index=0) const{
    return item[index].second <= timestamp_;
  }

 private:
  void release();

  MutablePropertyFragment& graph_;
  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
