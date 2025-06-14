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

#include <limits>
#include <utility>

#include "flex/storages/rt_mutable_graph/mutable_csr.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"
#include "flex/storages/rt_mutable_graph/types.h"

namespace gs {

class MutablePropertyFragment;
class VersionManager;

#if OV
template <typename EDATA_T>
class AdjListView {
  class nbr_iterator {
    using nbr_t = MutableNbr<EDATA_T>;

   public:
    nbr_iterator(const nbr_t* ptr, const nbr_t* end, timestamp_t timestamp)
        : ptr_(ptr), end_(end), timestamp_(timestamp) {
      while (ptr_ != end && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
    }

    const nbr_t& operator*() const { return *ptr_; }

    const nbr_t* operator->() const { return ptr_; }

    nbr_iterator& operator++() {
      ++ptr_;
      while (ptr_ != end_ && ptr_->timestamp > timestamp_) {
        ++ptr_;
      }
      return *this;
    }

    bool operator==(const nbr_iterator& rhs) const {
      return (ptr_ == rhs.ptr_);
    }

    bool operator!=(const nbr_iterator& rhs) const {
      return (ptr_ != rhs.ptr_);
    }

   private:
    const nbr_t* ptr_;
    const nbr_t* end_;
    timestamp_t timestamp_;
  };

 public:
  using slice_t = MutableNbrSlice<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(slice), timestamp_(timestamp) {}

  nbr_iterator begin() const {
    return nbr_iterator(edges_.begin(), edges_.end(), timestamp_);
  }
  nbr_iterator end() const {
    return nbr_iterator(edges_.end(), edges_.end(), timestamp_);
  }

  int estimated_degree() const { return edges_.size(); }

 private:
  slice_t edges_;
  timestamp_t timestamp_;
};
#else
template <typename EDATA_T>
class AdjListView {
 public:
  using slice_t = MutableNbrSlice<EDATA_T>;
  using sliceiter_t = TypedMutableCsrConstEdgeIter<EDATA_T>;

  AdjListView(const slice_t& slice, timestamp_t timestamp)
      : edges_(sliceiter_t(slice)), timestamp_(timestamp) {
    while (edges_.is_valid() && edges_.get_timestamp() > timestamp_) {
      edges_.next();
    }
  }

  AdjListView(const gbp::BufferBlock slice, int size, timestamp_t timestamp)
      : edges_(sliceiter_t(slice, size)), timestamp_(timestamp) {
    while (edges_.is_valid() && edges_.get_timestamp() > timestamp_) {
      edges_.next();
    }
  }

  FORCE_INLINE vid_t get_neighbor() { return edges_.get_neighbor(); }

  FORCE_INLINE const void* get_data() { return edges_.get_data(); }

  FORCE_INLINE timestamp_t get_timestamp() { return edges_.get_timestamp(); }

  FORCE_INLINE void next() {
    edges_.next();
    while (edges_.is_valid() && edges_.get_timestamp() > timestamp_) {
      edges_.next();
    }
  }

  FORCE_INLINE bool is_valid() {
    return edges_.is_valid() && edges_.get_timestamp() <= timestamp_;
  }
  FORCE_INLINE void recover() { edges_.recover(); }
  FORCE_INLINE void free() { edges_.free(); }
  int estimated_degree() const { return edges_.size(); }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  sliceiter_t edges_;
  timestamp_t timestamp_;
};
#endif
template <typename EDATA_T>
class GraphView {
 public:
  GraphView(const MutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}

  AdjListView<EDATA_T> get_edges(vid_t v) const {
    return AdjListView<EDATA_T>(csr_.get_edges(v), timestamp_);
  }
  timestamp_t timestamp() const { return timestamp_; }

 private:
  const MutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

template <typename EDATA_T>
class SingleGraphView {
  using nbr_t = MutableNbr<EDATA_T>;

 public:
  SingleGraphView(const SingleMutableCsr<EDATA_T>& csr, timestamp_t timestamp)
      : csr_(csr), timestamp_(timestamp) {}
#if OV
  bool exist(vid_t v) const {
    return (csr_.get_edge(v).timestamp.load() <= timestamp_);
  }

  const MutableNbr<EDATA_T>& get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }
#else
  FORCE_INLINE bool exist(vid_t v) const {
    auto item = csr_.get_edge(v);
    return (gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_);
  }

  FORCE_INLINE const gbp::BufferBlock exist(vid_t v, bool& exist) const {
    auto item = csr_.get_edge(v);
    exist = gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
    return item;
  }
  FORCE_INLINE bool exist1(gbp::BufferBlock& item) const {
    return gbp::BufferBlock::Ref<nbr_t>(item).timestamp.load() <= timestamp_;
  }
  FORCE_INLINE const gbp::BufferBlock get_edge(vid_t v) const {
    return csr_.get_edge(v);
  }

  FORCE_INLINE timestamp_t timestamp() const { return timestamp_; }
#endif
 private:
  const SingleMutableCsr<EDATA_T>& csr_;
  timestamp_t timestamp_;
};

class ReadTransaction {
 public:
  ReadTransaction(const MutablePropertyFragment& graph, VersionManager& vm,
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
#if OV
    Any GetField(int col_id) const;
#else
    gbp::BufferBlock GetField(int col_id) const;
#endif
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
#if OV
    Any GetData() const;
#else
    const void* GetData() const;
#endif
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

  bool GetVertexIndex(label_t label, oid_t id, vid_t& index) const;

  vid_t GetVertexNum(label_t label) const;

  oid_t GetVertexId(label_t label, vid_t index) const;

  edge_iterator GetOutEdgeIterator(label_t label, vid_t u,
                                   label_t neighnor_label,
                                   label_t edge_label) const;

  edge_iterator GetInEdgeIterator(label_t label, vid_t u,
                                  label_t neighnor_label,
                                  label_t edge_label) const;

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetOutgoingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));

    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  template <typename EDATA_T>
  AdjListView<EDATA_T> GetIncomingEdges(label_t v_label, vid_t v,
                                        label_t neighbor_label,
                                        label_t edge_label) const {
    auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return AdjListView<EDATA_T>(csr->get_edges(v), timestamp_);
  }

  const Schema& schema() const;

  template <typename EDATA_T>
  GraphView<EDATA_T> GetOutgoingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  GraphView<EDATA_T> GetIncomingGraphView(label_t v_label,
                                          label_t neighbor_label,
                                          label_t edge_label) const {
    auto csr = dynamic_cast<const MutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return GraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetOutgoingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_oe_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  template <typename EDATA_T>
  SingleGraphView<EDATA_T> GetIncomingSingleGraphView(
      label_t v_label, label_t neighbor_label, label_t edge_label) const {
    auto csr = dynamic_cast<const SingleMutableCsr<EDATA_T>*>(
        graph_.get_ie_csr(v_label, neighbor_label, edge_label));
    return SingleGraphView<EDATA_T>(*csr, timestamp_);
  }

  // ========================== batching 接口 ==========================

#if !OV
  /**
  批量获取顶点ID
  @param label: 顶点标签
  @param indices: 顶点索引组成的vector
  @return: 顶点ID组成的vector
  */
  std::vector<oid_t> BatchGetVertexIds(label_t label,
                                       const std::vector<vid_t>& indices) const;
  /**
  批量查找顶点索引
  @param label: 顶点标签
  @param oids: 顶点ID组成的vector
  @return: 顶点索引组成的vector和是否存在的vector
  */
  std::pair<std::vector<vid_t>, std::vector<bool>> BatchGetVertexIndices(
      label_t label, const std::vector<oid_t>& oids) const;

  /**
  批量获取顶点属性
  @param label_id: 顶点标签
  @param vids: 顶点ID组成的vector
  @param prop_names: 属性名称组成的vector
  @return: 顶点属性组成的vector
  */
  std::vector<std::vector<gbp::BufferBlock>> BatchGetVertexPropsFromVids(
      label_t label_id, const std::vector<vid_t>& vids,
      const std::vector<std::string>& prop_names) const;
  // std::vector<std::vector<gbp::BufferBlock>> BatchGetVertexPropsFromVids(
  //     label_t label_id, const std::vector<vid_t>& vids,
  //     const std::vector<ColumnBase*>& prop_columns) const;

  /** 批量获取出边
  @param v_label: 顶点标签
  @param vids: 顶点ID组成的vector
  @param neighbor_label: 邻居标签
  @param edge_label: 边标签
  @return: 出边列表
  */
  template <typename EDATA_T>
  std::vector<AdjListView<EDATA_T>> BatchGetOutgoingEdges(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      const std::vector<vid_t>& vids) const {
    std::vector<gbp::BufferBlock> adj_blocks;
    adj_blocks.reserve(vids.size());
    std::vector<gbp::BufferBlock> edge_blocks;
    edge_blocks.reserve(vids.size());

    std::vector<gbp::batch_request_type> requests;
    std::vector<AdjListView<EDATA_T>> results;
    // results.reserve(vids.size());
    // 获取所有邻接列表
    for (auto v : vids) {
      auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
          graph_.get_oe_csr(v_label, neighbor_label, edge_label));
      requests.emplace_back(csr->get_edgelist_batch(v));
    }
    buffer_pool_manager_->GetBlockBatch(requests, adj_blocks);

    // 获取所有边列表
    requests.clear();
    for (size_t i = 0; i < adj_blocks.size(); ++i) {
      auto& adj_list =
          gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adj_blocks[i]);
      auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
          graph_.get_oe_csr(v_label, neighbor_label, edge_label));
      requests.emplace_back(
          csr->get_edges_batch(adj_list.start_idx_, adj_list.size_));
    }
    buffer_pool_manager_->GetBlockBatch(requests, edge_blocks);

    // 生成最后结果
    for (size_t i = 0; i < adj_blocks.size(); ++i) {
      results.emplace_back(
          edge_blocks[i],
          gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adj_blocks[i]).size_,
          timestamp_);
    }

    return std::move(results);
  }

  /** 批量获取入边
  @param v_label: 顶点标签
  @param vids: 顶点ID组成的vector
  @param neighbor_label: 邻居标签
  @param edge_label: 边标签
  @return: 入边列表
  */
  template <typename EDATA_T>
  std::vector<AdjListView<EDATA_T>> BatchGetIncomingEdges(
      label_t v_label, label_t neighbor_label, label_t edge_label,
      const std::vector<vid_t>& vids) const {
    std::vector<gbp::BufferBlock> adj_blocks;
    adj_blocks.reserve(vids.size());
    std::vector<gbp::BufferBlock> edge_blocks;
    edge_blocks.reserve(vids.size());

    std::vector<gbp::batch_request_type> requests;
    std::vector<AdjListView<EDATA_T>> results;
    // results.reserve(vids.size());
    // 获取所有邻接列表
    for (auto v : vids) {
      auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
          graph_.get_ie_csr(v_label, neighbor_label, edge_label));
      requests.emplace_back(csr->get_edgelist_batch(v));
    }
    buffer_pool_manager_->GetBlockBatch(requests, adj_blocks);
    requests.clear();

    // 获取所有边列表
    for (size_t i = 0; i < adj_blocks.size(); ++i) {
      auto& adj_list =
          gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adj_blocks[i]);
      auto csr = dynamic_cast<const TypedMutableCsrBase<EDATA_T>*>(
          graph_.get_ie_csr(v_label, neighbor_label, edge_label));
      requests.emplace_back(
          csr->get_edges_batch(adj_list.start_idx_, adj_list.size_));
    }

    buffer_pool_manager_->GetBlockBatch(requests, edge_blocks);

    // 生成最后结果
    for (size_t i = 0; i < adj_blocks.size(); ++i) {
      results.emplace_back(
          edge_blocks[i],
          gbp::BufferBlock::Ref<MutableAdjlist<EDATA_T>>(adj_blocks[i]).size_,
          timestamp_);
    }

    return std::move(results);
  }

  std::vector<gbp::BufferBlock> BatchGetOutgoingSingleEdges(
      const label_t& v_label, const label_t& neighbor_label,
      const label_t& edge_label, const std::vector<vid_t>& vids) const;

  std::vector<gbp::BufferBlock> BatchGetIncomingSingleEdges(
      const label_t& v_label, const label_t& neighbor_label,
      const label_t& edge_label, const std::vector<vid_t>& vids) const;

  // template <typename EDATA_T>
  // std::vector<gbp::BufferBlock> BatchGetEdgePropsFromSrcVids(
  //     const label_t& v_label, const label_t& neighbor_label,
  //     const label_t& edge_label, const std::vector<vid_t>& vids,
  //     bool is_out) const;
#endif
  // ========================== batching 接口 ==========================

 private:
  void release();

  const MutablePropertyFragment& graph_;
  gbp::BufferPoolManager* buffer_pool_manager_ = nullptr;

  VersionManager& vm_;
  timestamp_t timestamp_;
};

}  // namespace gs

#endif  // GRAPHSCOPE_DATABASE_READ_TRANSACTION_H_
