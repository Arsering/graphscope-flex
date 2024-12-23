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

#include "flex/storages/rt_mutable_graph/types.h"
#include "grape/util.h"

#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include "flex/engines/hqps_db/core/utils/hqps_utils.h"
#include "flex/graphscope_bufferpool/include/buffer_pool_manager.h"
#include "flex/storages/rt_mutable_graph/file_names.h"
#include "flex/utils/property/types.h"

namespace gs {

MutablePropertyFragment::MutablePropertyFragment() {}

MutablePropertyFragment::~MutablePropertyFragment() {
  std::vector<size_t> degree_list(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    degree_list[i] = lf_indexers_[i].size();
    vertex_data_[i].resize(degree_list[i]);
  }
  for (size_t src_label = 0; src_label != vertex_label_num_; ++src_label) {
    for (size_t dst_label = 0; dst_label != vertex_label_num_; ++dst_label) {
      for (size_t e_label = 0; e_label != edge_label_num_; ++e_label) {
        size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                       dst_label * edge_label_num_ + e_label;
        if (ie_[index] != NULL) {
          ie_[index]->resize(degree_list[dst_label]);
          delete ie_[index];
        }
        if (oe_[index] != NULL) {
          oe_[index]->resize(degree_list[src_label]);
          delete oe_[index];
        }
      }
    }
  }
}

void MutablePropertyFragment::loadSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open();
  schema_.Deserialize(io_adaptor);
}

void MutablePropertyFragment::DumpSchema(const std::string& schema_path) {
  auto io_adaptor = std::unique_ptr<grape::LocalIOAdaptor>(
      new grape::LocalIOAdaptor(schema_path));
  io_adaptor->Open("wb");
  schema_.Serialize(io_adaptor);
  io_adaptor->Close();
}

inline MutableCsrBase* create_csr(EdgeStrategy es,
                                  const std::vector<PropertyType>& properties) {
  if (properties.empty()) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<grape::EmptyType>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<grape::EmptyType>();
    }
  } else if (properties[0] == PropertyType::kInt32) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<int>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<int>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int>();
    }
  } else if (properties[0] == PropertyType::kDate) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<Date>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<Date>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<Date>();
    }
  } else if (properties[0] == PropertyType::kInt64) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<int64_t>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<int64_t>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<int64_t>();
    }
  } else if (properties[0] == PropertyType::kDouble) {
    if (es == EdgeStrategy::kSingle) {
      return new SingleMutableCsr<double>();
    } else if (es == EdgeStrategy::kMultiple) {
      return new MutableCsr<double>();
    } else if (es == EdgeStrategy::kNone) {
      return new EmptyCsr<double>();
    }
  }
  LOG(FATAL) << "not support edge strategy or edge data type";
  return nullptr;
}

std::string get_latest_snapshot(const std::string& work_dir) {
  std::string snapshots_dir = work_dir + "/snapshots";
  uint32_t version;
  {
    FILE* fin = fopen((snapshots_dir + "/VERSION").c_str(), "r");
    auto ret = ::fread(&version, sizeof(uint32_t), 1, fin);
  }
  return snapshots_dir + "/" + std::to_string(version);
}

void MutablePropertyFragment::Open(const std::string& work_dir) {
  loadSchema(schema_path(work_dir));
  vertex_label_num_ = schema_.vertex_label_num();
  edge_label_num_ = schema_.edge_label_num();

  lf_indexers_.resize(vertex_label_num_);
  vertex_data_.resize(vertex_label_num_);
  std::string snapshot_dir = get_latest_snapshot(work_dir);
  std::string tmp_dir_path = tmp_dir(work_dir);
  std::filesystem::create_directory(tmp_dir_path);
  std::vector<size_t> vertex_capacities(vertex_label_num_, 0);

  auto t0 = -grape::GetCurrentTime();
  size_t size_in_byte_lf_indexer = 0;
  size_t size_in_byte_vertex_data = 0;
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    std::string v_label_name = schema_.get_vertex_label_name(i);
    lf_indexers_[i].open(vertex_map_prefix(v_label_name), snapshot_dir,
                         tmp_dir_path);

    size_in_byte_lf_indexer += lf_indexers_[i].get_size_in_byte();
    vertex_data_[i].open(vertex_table_prefix(v_label_name), snapshot_dir,
                         tmp_dir_path, schema_.get_vertex_property_names(i),
                         schema_.get_vertex_properties(i),
                         schema_.get_vertex_storage_strategies(v_label_name));

    size_in_byte_vertex_data += vertex_data_[i].get_size_in_byte();

    size_t vertex_num = lf_indexers_[i].size();
    size_t vertex_capacity = vertex_num;
    // TODO:
    vertex_capacity += vertex_capacity >> 2;
    vertex_data_[i].resize(vertex_capacity);
    vertex_capacities[i] = vertex_capacity;
  }
  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Time used = " << t0;
  size_t MB_in_byte = 1024LU * 1024;
  LOG(INFO) << "size_in_MB_lf_indexer=" << size_in_byte_lf_indexer / MB_in_byte
            << " | size_in_MB_vertex_data="
            << size_in_byte_vertex_data / MB_in_byte;

  ie_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);
  oe_.resize(vertex_label_num_ * vertex_label_num_ * edge_label_num_, NULL);

  t0 = -grape::GetCurrentTime();
  size_t size_in_byte_edge_index = 0;
  size_t size_in_byte_edge_data = 0;
  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index = src_label_i * vertex_label_num_ * edge_label_num_ +
                       dst_label_i * edge_label_num_ + e_label_i;
        auto& properties =
            schema_.get_edge_properties(src_label, dst_label, edge_label);
        EdgeStrategy oe_strategy = schema_.get_outgoing_edge_strategy(
            src_label, dst_label, edge_label);
        EdgeStrategy ie_strategy = schema_.get_incoming_edge_strategy(
            src_label, dst_label, edge_label);
        ie_[index] = create_csr(ie_strategy, properties);
        oe_[index] = create_csr(oe_strategy, properties);
        ie_[index]->open(ie_prefix(src_label, dst_label, edge_label),
                         snapshot_dir, tmp_dir_path);
        ie_[index]->resize(vertex_capacities[dst_label_i]);
        oe_[index]->open(oe_prefix(src_label, dst_label, edge_label),
                         snapshot_dir, tmp_dir_path);

        oe_[index]->resize(vertex_capacities[src_label_i]);
        size_in_byte_edge_index += ie_[index]->get_index_size_in_byte();
        size_in_byte_edge_index += oe_[index]->get_index_size_in_byte();
        size_in_byte_edge_data += ie_[index]->get_data_size_in_byte();
        size_in_byte_edge_data += oe_[index]->get_data_size_in_byte();
      }
    }
  }
  t0 += grape::GetCurrentTime();
  LOG(INFO) << "Time used = " << t0;
  LOG(INFO) << "size_in_MB_edge_index=" << size_in_byte_edge_index / MB_in_byte
            << " | size_in_MB_edge_data="
            << size_in_byte_edge_data / MB_in_byte;
}

void MutablePropertyFragment::Dump(const std::string& work_dir,
                                   uint32_t version) {
  std::string snapshot_dir_path = snapshot_dir(work_dir, version);
  std::filesystem::create_directories(snapshot_dir_path);
  std::vector<size_t> vertex_num(vertex_label_num_, 0);
  for (size_t i = 0; i < vertex_label_num_; ++i) {
    vertex_num[i] = lf_indexers_[i].size();
    lf_indexers_[i].dump(vertex_map_prefix(schema_.get_vertex_label_name(i)),
                         snapshot_dir_path);
    vertex_data_[i].resize(vertex_num[i]);
    vertex_data_[i].dump(vertex_table_prefix(schema_.get_vertex_label_name(i)),
                         snapshot_dir_path);
  }

  for (size_t src_label_i = 0; src_label_i != vertex_label_num_;
       ++src_label_i) {
    std::string src_label =
        schema_.get_vertex_label_name(static_cast<label_t>(src_label_i));
    for (size_t dst_label_i = 0; dst_label_i != vertex_label_num_;
         ++dst_label_i) {
      std::string dst_label =
          schema_.get_vertex_label_name(static_cast<label_t>(dst_label_i));
      for (size_t e_label_i = 0; e_label_i != edge_label_num_; ++e_label_i) {
        std::string edge_label =
            schema_.get_edge_label_name(static_cast<label_t>(e_label_i));
        if (!schema_.exist(src_label, dst_label, edge_label)) {
          continue;
        }
        size_t index = src_label_i * vertex_label_num_ * edge_label_num_ +
                       dst_label_i * edge_label_num_ + e_label_i;
        if (ie_[index] != NULL) {
          ie_[index]->resize(vertex_num[dst_label_i]);
          ie_[index]->dump(ie_prefix(src_label, dst_label, edge_label),
                           snapshot_dir_path);
        }
        if (oe_[index] != NULL) {
          oe_[index]->resize(vertex_num[src_label_i]);
          oe_[index]->dump(oe_prefix(src_label, dst_label, edge_label),
                           snapshot_dir_path);
        }
      }
    }
  }
  set_snapshot_version(work_dir, version);
}

void MutablePropertyFragment::IngestEdge(label_t src_label, vid_t src_lid,
                                         label_t dst_label, vid_t dst_lid,
                                         label_t edge_label, timestamp_t ts,
                                         grape::OutArchive& arc,
                                         MMapAllocator& alloc) {
  size_t index = src_label * vertex_label_num_ * edge_label_num_ +
                 dst_label * edge_label_num_ + edge_label;
  ie_[index]->peek_ingest_edge(dst_lid, src_lid, arc, ts, alloc);
  oe_[index]->ingest_edge(src_lid, dst_lid, arc, ts, alloc);
}

const Schema& MutablePropertyFragment::schema() const { return schema_; }

Table& MutablePropertyFragment::get_vertex_table(label_t vertex_label) {
  return vertex_data_[vertex_label];
}

const Table& MutablePropertyFragment::get_vertex_table(
    label_t vertex_label) const {
  return vertex_data_[vertex_label];
}

vid_t MutablePropertyFragment::vertex_num(label_t vertex_label) const {
  return static_cast<vid_t>(cgraph_lf_indexers_[vertex_label]->size());
}

bool MutablePropertyFragment::get_lid(label_t label, oid_t oid,
                                      vid_t& lid) const {
  return cgraph_lf_indexers_[label]->get_index(oid, lid);
}

oid_t MutablePropertyFragment::get_oid(label_t label, vid_t lid) const {
  return cgraph_lf_indexers_[label]->get_key(lid);
}

vid_t MutablePropertyFragment::add_vertex(label_t label, oid_t id) {
  return cgraph_lf_indexers_[label]->insert(id);
}

std::shared_ptr<MutableCsrConstEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter(u);
}

std::shared_ptr<MutableCsrConstEdgeIterBase>
MutablePropertyFragment::get_incoming_edges(label_t label, vid_t u,
                                            label_t neighbor_label,
                                            label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter(u);
}

MutableCsrConstEdgeIterBase* MutablePropertyFragment::get_outgoing_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter_raw(u);
}

MutableCsrConstEdgeIterBase* MutablePropertyFragment::get_incoming_edges_raw(
    label_t label, vid_t u, label_t neighbor_label, label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter_raw(u);
}

std::shared_ptr<MutableCsrEdgeIterBase>
MutablePropertyFragment::get_outgoing_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index]->edge_iter_mut(u);
}

std::shared_ptr<MutableCsrEdgeIterBase>
MutablePropertyFragment::get_incoming_edges_mut(label_t label, vid_t u,
                                                label_t neighbor_label,
                                                label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index]->edge_iter_mut(u);
}

MutableCsrBase* MutablePropertyFragment::get_oe_csr(label_t label,
                                                    label_t neighbor_label,
                                                    label_t edge_label) {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

const MutableCsrBase* MutablePropertyFragment::get_oe_csr(
    label_t label, label_t neighbor_label, label_t edge_label) const {
  size_t index = label * vertex_label_num_ * edge_label_num_ +
                 neighbor_label * edge_label_num_ + edge_label;
  return oe_[index];
}

MutableCsrBase* MutablePropertyFragment::get_ie_csr(label_t label,
                                                    label_t neighbor_label,
                                                    label_t edge_label) {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

const MutableCsrBase* MutablePropertyFragment::get_ie_csr(
    label_t label, label_t neighbor_label, label_t edge_label) const {
  size_t index = neighbor_label * vertex_label_num_ * edge_label_num_ +
                 label * edge_label_num_ + edge_label;
  return ie_[index];
}

void MutablePropertyFragment::cgraph_open(
    const std::string& snapshot_dir_path) {
  loadSchema(
      schema_path(snapshot_dir_path + "/snapshots/" + std::to_string(1)));

  std::filesystem::create_directories(snapshot_dir_path +
                                      "/runtime/tmp");  // 创建runtime/tmp目录
  LOG(INFO) << "copy snapshot from "
            << snapshot_dir_path + "/snapshots/" + std::to_string(1) << " to "
            << snapshot_dir_path + "/runtime/tmp";
  std::filesystem::copy(snapshot_dir_path + "/snapshots/" + std::to_string(1),
                        snapshot_dir_path + "/runtime/tmp",
                        std::filesystem::copy_options::recursive |
                            std::filesystem::copy_options::overwrite_existing);
  LOG(INFO) << "copy snapshot to tmp done";

  std::map<size_t, std::vector<std::pair<size_t, size_t>>>
      parent_configs;  // vertex_id -> (child_vertex_id, group_size)
  std::map<size_t, std::pair<size_t, size_t>>
      child_configs;  // vertex_id -> (parent_vertex_id, label_Id_in_parent,)
  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    if (schema_.group_foreign_keys_1_.count(vertex_id) == 0) {
      continue;
    }
    auto& foreign_key_1 = schema_.group_foreign_keys_1_.at(vertex_id);
    auto& foreign_key_2 = schema_.group_foreign_keys_2_.at(vertex_id);
    auto child_vertex_id = vertex_id;
    auto group_size = std::get<1>(foreign_key_2);
    auto parent_vertex_id =
        schema_.get_vertex_label_id(std::get<0>(foreign_key_2));
    auto label_id_in_parent = parent_configs[parent_vertex_id].size() - 1;
    parent_configs[parent_vertex_id].emplace_back(child_vertex_id, group_size);
    child_configs[child_vertex_id] =
        std::make_pair(parent_vertex_id, label_id_in_parent);
  }

  for (size_t vertex_id = 0;
       vertex_id < schema_.vprop_column_family_nums_.size(); vertex_id++) {
    if (child_configs.count(vertex_id) == 1 &&
        parent_configs.count(vertex_id) == 1) {
      assert(false);
    }
    if (child_configs.count(vertex_id) == 0 &&
        parent_configs.count(vertex_id) == 0) {
      LFIndexer<vid_t>* lf_indexer = new LFIndexer<vid_t>();
      lf_indexer->open("hash_map",
                       snapshot_dir_path + "/snapshots/" + std::to_string(1) +
                           "/" + schema_.get_vertex_label_name(vertex_id),
                       snapshot_dir_path + "/runtime/tmp/" +
                           schema_.get_vertex_label_name(vertex_id));
      cgraph_lf_indexers_.emplace_back(lf_indexer);
    } else if (child_configs.count(vertex_id) == 1) {
      GroupedChildLFIndexer<vid_t>* lf_indexer =
          new GroupedChildLFIndexer<vid_t>();
      lf_indexer->open("hash_map",
                       snapshot_dir_path + "/snapshots/" + std::to_string(1) +
                           "/" + schema_.get_vertex_label_name(vertex_id),
                       snapshot_dir_path + "/runtime/tmp/" +
                           schema_.get_vertex_label_name(vertex_id));
      cgraph_lf_indexers_.emplace_back(lf_indexer);
    } else {
      GroupedParentLFIndexer<vid_t, 2>* lf_indexer =
          new GroupedParentLFIndexer<vid_t, 2>();
      lf_indexer->open("hash_map",
                       snapshot_dir_path + "/snapshots/" + std::to_string(1) +
                           "/" + schema_.get_vertex_label_name(vertex_id),
                       snapshot_dir_path + "/runtime/tmp/" +
                           schema_.get_vertex_label_name(vertex_id));
      cgraph_lf_indexers_.emplace_back(lf_indexer);
    }
    // create vertex
    vertices_.emplace_back();
    // 初始化vertex
    vertices_.back().Open(schema_.get_vertex_label_name(vertex_id),
                          snapshot_dir_path + "/runtime/tmp");
  }

  LOG(INFO) << "open vertex done";

  size_t edge_size = 0;

  auto person_label_id = schema_.get_vertex_label_id("PERSON");
  auto property_id = 5;
  gs::oid_t person_oid = 8796093022290;
  gs::vid_t person_vid =
      cgraph_lf_indexers_[person_label_id]->get_index(person_oid);
  assert(person_oid ==
         cgraph_lf_indexers_[person_label_id]->get_key(person_vid));
  auto item = vertices_[person_label_id].ReadColumn(person_vid, property_id);
  std::vector<char> data(item.Size());
  item.Copy(data.data(), data.size());
  LOG(INFO) << "data: " << std::string_view(data.data(), data.size());
  LOG(INFO) << "data: " << gbp::BufferBlock::Ref<gs::Date>(item).milli_second;
  LOG(INFO) << gbp::TimeConverter::millisToDateString(
      gbp::BufferBlock::Ref<gs::Date>(item).milli_second, true);

  auto edge_label_id_with_direction =
      schema_.generate_edge_label_with_direction(
          person_label_id, person_label_id, schema_.get_edge_label_id("KNOWS"),
          false);
  auto item_t = vertices_[person_label_id].ReadEdges(
      person_vid, edge_label_id_with_direction, edge_size);
  LOG(INFO) << "edge_size: " << edge_size;
  for (int i = 0; i < edge_size; i++) {  LOG(INFO) << "cp"
            << gbp::BufferBlock::Ref<MutableNbr<gs::Date>>(item_t).neighbor;
    LOG(INFO) << "data: "
              << cgraph_lf_indexers_[person_label_id]->get_key(
                   gbp::BufferBlock::Ref<MutableNbr<gs::Date>>(item_t).neighbor)
            << " "
            << gbp::TimeConverter::millisToDateString(
                     gbp::BufferBlock::Ref<MutableNbr<gs::Date>>(item_t, i)
                         .data.milli_second,
                     true);
  }
  person_oid = 1129;
  auto person_vid2 = cgraph_lf_indexers_[person_label_id]->get_index(person_oid);

  auto post_label_id = schema_.get_vertex_label_id("POST");
  auto edge_label_id_with_direction2 =
      schema_.generate_edge_label_with_direction(
          post_label_id, person_label_id,
          schema_.get_edge_label_id("HASCREATOR"), false);
  auto item_t2 = vertices_[person_label_id].ReadEdges(
      person_vid2, edge_label_id_with_direction2, edge_size);
  LOG(INFO) << "edge_size2: " << edge_size;
  for (int i = 0; i < edge_size; i++) {
    auto post_id =
        gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item_t2, i)
            .neighbor;
    LOG(INFO) << "post oid is " << cgraph_lf_indexers_[post_label_id]->get_key(post_id);
  }

  auto comment_label_id = schema_.get_vertex_label_id("COMMENT");
  auto edge_label_id_with_direction3 =
      schema_.generate_edge_label_with_direction(
          comment_label_id, person_label_id,
          schema_.get_edge_label_id("HASCREATOR"), false);
  auto item_t3 = vertices_[person_label_id].ReadEdges(
      person_vid2, edge_label_id_with_direction3, edge_size);
  for (int i = 0; i < edge_size; i++) {
    auto comment_id =
        gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item_t3, i)
            .neighbor;
    LOG(INFO) << "comment oid is "
              << cgraph_lf_indexers_[comment_label_id]->get_key(comment_id);
  }

  {  // test vertex property
    std::vector<gs::oid_t> person_oid_list = {933,1129,6597069767117};
    for (auto person_oid : person_oid_list) {
      auto person_vid = cgraph_lf_indexers_[person_label_id]->get_index(person_oid);
      for (auto property_id = 1; property_id < 9; property_id++) {
        if (property_id == 4 || property_id == 5) {
          continue;
        }
        auto item_p = vertices_[person_label_id].ReadColumn(person_vid, property_id);
        std::vector<char> data(item_p.Size());
        item_p.Copy(data.data(), data.size());
        LOG(INFO) << "data: " << std::string_view(data.data(), data.size());
      }
    }

    std::vector<gs::oid_t> comment_oid_list = {618475290625,1030792151054};
    for (auto comment_oid : comment_oid_list) {
      auto comment_vid = cgraph_lf_indexers_[comment_label_id]->get_index(comment_oid);
      for (auto property_id = 1; property_id < 5; property_id++) {
        if (property_id == 1) {
          continue;
        }
        auto item_p = vertices_[comment_label_id].ReadColumn(comment_vid, property_id);
        std::vector<char> data(item_p.Size());
        item_p.Copy(data.data(), data.size());
        LOG(INFO) << "data: " << std::string_view(data.data(), data.size());
      }
    }

    std::vector<gs::oid_t> post_oid_list = {618475290624,481036337190};
    for (auto post_oid : post_oid_list) {
      auto post_vid = cgraph_lf_indexers_[post_label_id]->get_index(post_oid);
      for (auto property_id = 1; property_id < 7; property_id++) {
        if (property_id == 2) {
          continue;
        }
        auto item_p = vertices_[post_label_id].ReadColumn(post_vid, property_id);
        std::vector<char> data(item_p.Size());
        if (data.size() > 0) {
          item_p.Copy(data.data(), data.size());
          LOG(INFO) << "data: " << std::string_view(data.data(), data.size());
        }else {
          LOG(INFO) << "data is empty";
        }
      }
    }

  }

  {//test person knows person
    LOG(INFO) << "test person knows person";
    std::vector<gs::oid_t> person_oid_list = {933,1129,6597069767117};
    for (auto person_oid : person_oid_list) {
      auto person_vid = cgraph_lf_indexers_[person_label_id]->get_index(person_oid);
      auto knows_edge_label_id_with_direction_out =
          schema_.generate_edge_label_with_direction(
              person_label_id, person_label_id, schema_.get_edge_label_id("KNOWS"),
              true);
      auto item_t4 = vertices_[person_label_id].ReadEdges(
          person_vid, knows_edge_label_id_with_direction_out, edge_size);
      for (int i = 0; i < edge_size; i++) {
        auto knows_id =
            gbp::BufferBlock::Ref<MutableNbr<gs::Date>>(item_t4, i)
                .neighbor;
        LOG(INFO)<<person_oid<< "out knows lid is "<<knows_id;
        if(knows_id<1520){
          LOG(INFO)<<person_oid<< "out knows oid is "
                  << cgraph_lf_indexers_[person_label_id]->get_key(knows_id);
        }
      }
      auto knows_edge_label_id_with_direction_in =
          schema_.generate_edge_label_with_direction(
              person_label_id, person_label_id, schema_.get_edge_label_id("KNOWS"),
              false);
      auto item_t5 = vertices_[person_label_id].ReadEdges(
          person_vid, knows_edge_label_id_with_direction_in, edge_size);
      for (int i = 0; i < edge_size; i++) {
        auto knows_id =
            gbp::BufferBlock::Ref<MutableNbr<gs::Date>>(item_t5, i)
                .neighbor;
        LOG(INFO)<<person_oid<< "in knows lid is "<<knows_id;
        if(knows_id<1520){
          LOG(INFO)<<person_oid<< "in knows oid is "
                  << cgraph_lf_indexers_[person_label_id]->get_key(knows_id);
        }
      }
    }
  }

  {
    LOG(INFO) << "test comment reply post timestamp";
    auto comment_label_id = schema_.get_vertex_label_id("COMMENT");
    auto post_label_id = schema_.get_vertex_label_id("POST");
    auto comment_oid = 1030792475784;
    auto comment_vid =
        cgraph_lf_indexers_[comment_label_id]->get_index(comment_oid);
    auto comment_reply_post_edge_label_id_with_direction3 =
        schema_.generate_edge_label_with_direction(
            comment_label_id, post_label_id,
            schema_.get_edge_label_id("REPLYOF"), true);
    auto item_t9 = vertices_[comment_label_id].ReadEdges(
        comment_vid, comment_reply_post_edge_label_id_with_direction3,
        edge_size);
    auto timestamp =
        gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item_t9, 0)
            .timestamp.load();
    LOG(INFO) << "post timestamp: " << timestamp;
    auto comment_reply_comment_edge_label_id_with_direction4 =
        schema_.generate_edge_label_with_direction(
            comment_label_id, comment_label_id,
            schema_.get_edge_label_id("REPLYOF"), true);
    auto item_t10 = vertices_[comment_label_id].ReadEdges(
        comment_vid, comment_reply_comment_edge_label_id_with_direction4,
        edge_size);
    auto timestamp2 =
        gbp::BufferBlock::Ref<MutableNbr<grape::EmptyType>>(item_t10, 0)
            .timestamp.load();
    LOG(INFO) << "comment timestamp: " << timestamp2;
  }
}

}  // namespace gs
