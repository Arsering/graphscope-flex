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

#include "flex/storages/rt_mutable_graph/schema.h"

#include <yaml-cpp/yaml.h>

namespace gs {

Schema::Schema() = default;
Schema::~Schema() = default;

void Schema::add_vertex_label(const std::string& label,
                              const std::vector<PropertyType>& properties,
                              const std::vector<StorageStrategy>& strategies,
                              size_t max_vnum) {
  label_t v_label_id = vertex_label_to_index(label);
  vproperties_[v_label_id] = properties;
  vprop_storage_[v_label_id] = strategies;
  vprop_storage_[v_label_id].resize(vproperties_[v_label_id].size(),
                                    StorageStrategy::kMem);
  max_vnum_[v_label_id] = max_vnum;
}

void Schema::add_edge_label(const std::string& src_label,
                            const std::string& dst_label,
                            const std::string& edge_label,
                            const std::vector<PropertyType>& properties,
                            EdgeStrategy oe, EdgeStrategy ie) {
  label_t src_label_id = vertex_label_to_index(src_label);
  label_t dst_label_id = vertex_label_to_index(dst_label);
  label_t edge_label_id = edge_label_to_index(edge_label);

  uint32_t label_id =
      generate_edge_label(src_label_id, dst_label_id, edge_label_id);
  eproperties_[label_id] = properties;
  oe_strategy_[label_id] = oe;
  ie_strategy_[label_id] = ie;
}

label_t Schema::vertex_label_num() const {
  return static_cast<label_t>(vlabel_indexer_.size());
}

label_t Schema::edge_label_num() const {
  return static_cast<label_t>(elabel_indexer_.size());
}

bool Schema::contains_vertex_label(const std::string& label) const {
  label_t ret;
  return vlabel_indexer_.get_index(label, ret);
}

label_t Schema::get_vertex_label_id(const std::string& label) const {
  label_t ret;
  CHECK(vlabel_indexer_.get_index(label, ret));
  return ret;
}

void Schema::set_vertex_properties(
    label_t label_id, const std::vector<PropertyType>& types,
    const std::vector<StorageStrategy>& strategies) {
  vproperties_[label_id] = types;
  vprop_storage_[label_id] = strategies;
  vprop_storage_[label_id].resize(types.size(), StorageStrategy::kMem);
}

const std::vector<PropertyType>& Schema::get_vertex_properties(
    const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return vproperties_[index];
}

const std::vector<PropertyType>& Schema::get_vertex_properties(
    label_t label) const {
  return vproperties_[label];
}

const std::vector<StorageStrategy>& Schema::get_vertex_storage_strategies(
    const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return vprop_storage_[index];
}

size_t Schema::get_max_vnum(const std::string& label) const {
  label_t index;
  CHECK(vlabel_indexer_.get_index(label, index));
  return max_vnum_[index];
}

bool Schema::exist(const std::string& src_label, const std::string& dst_label,
                   const std::string& edge_label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(edge_label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

const std::vector<PropertyType>& Schema::get_edge_properties(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.at(index);
}

PropertyType Schema::get_edge_property(label_t src, label_t dst,
                                       label_t edge) const {
  uint32_t index = generate_edge_label(src, dst, edge);
  auto& vec = eproperties_.at(index);
  return vec.empty() ? PropertyType::kEmpty : vec[0];
}

bool Schema::valid_edge_property(const std::string& src_label,
                                 const std::string& dst_label,
                                 const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return eproperties_.find(index) != eproperties_.end();
}

EdgeStrategy Schema::get_outgoing_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return oe_strategy_.at(index);
}

EdgeStrategy Schema::get_incoming_edge_strategy(
    const std::string& src_label, const std::string& dst_label,
    const std::string& label) const {
  label_t src, dst, edge;
  CHECK(vlabel_indexer_.get_index(src_label, src));
  CHECK(vlabel_indexer_.get_index(dst_label, dst));
  CHECK(elabel_indexer_.get_index(label, edge));
  uint32_t index = generate_edge_label(src, dst, edge);
  return ie_strategy_.at(index);
}

label_t Schema::get_edge_label_id(const std::string& label) const {
  label_t ret;
  CHECK(elabel_indexer_.get_index(label, ret));
  return ret;
}

bool Schema::contains_edge_label(const std::string& label) const {
  label_t ret;
  return elabel_indexer_.get_index(label, ret);
}

std::string Schema::get_vertex_label_name(label_t index) const {
  std::string ret;
  vlabel_indexer_.get_key(index, ret);
  return ret;
}

std::string Schema::get_edge_label_name(label_t index) const {
  std::string ret;
  elabel_indexer_.get_key(index, ret);
  return ret;
}

void Schema::Serialize(std::unique_ptr<grape::LocalIOAdaptor>& writer) {
  vlabel_indexer_.Serialize(writer);
  elabel_indexer_.Serialize(writer);
  grape::InArchive arc;
  arc << vproperties_ << vprop_storage_ << eproperties_ << ie_strategy_
      << oe_strategy_ << max_vnum_;
  CHECK(writer->WriteArchive(arc));
}

void Schema::Deserialize(std::unique_ptr<grape::LocalIOAdaptor>& reader) {
  vlabel_indexer_.Deserialize(reader);
  elabel_indexer_.Deserialize(reader);
  grape::OutArchive arc;
  CHECK(reader->ReadArchive(arc));
  arc >> vproperties_ >> vprop_storage_ >> eproperties_ >> ie_strategy_ >>
      oe_strategy_ >> max_vnum_;
}

label_t Schema::vertex_label_to_index(const std::string& label) {
  label_t ret;
  vlabel_indexer_.add(label, ret);
  if (vproperties_.size() <= ret) {
    vproperties_.resize(ret + 1);
    vprop_storage_.resize(ret + 1);
    max_vnum_.resize(ret + 1);
  }
  return ret;
}

label_t Schema::edge_label_to_index(const std::string& label) {
  label_t ret;
  elabel_indexer_.add(label, ret);
  return ret;
}

uint32_t Schema::generate_edge_label(label_t src, label_t dst,
                                     label_t edge) const {
  uint32_t ret = 0;
  ret |= src;
  ret <<= 8;
  ret |= dst;
  ret <<= 8;
  ret |= edge;
  return ret;
}

namespace config_parsing {

template <typename T>
bool get_scalar(YAML::Node node, const std::string& key, T& value) {
  YAML::Node cur = node[key];
  if (cur && cur.IsScalar()) {
    value = cur.as<T>();
    return true;
  }
  return false;
}

template <typename T>
bool get_sequence(YAML::Node node, const std::string& key,
                  std::vector<T>& seq) {
  YAML::Node cur = node[key];
  if (cur && cur.IsSequence()) {
    int num = cur.size();
    seq.clear();
    for (int i = 0; i < num; ++i) {
      seq.push_back(cur[i].as<T>());
    }
    return true;
  }
  return false;
}

static bool expect_config(YAML::Node root, const std::string& key,
                          const std::string& value) {
  std::string got;
  if (!get_scalar(root, key, got)) {
    LOG(ERROR) << key << " not set properly...";
    return false;
  }
  if (got != value) {
    LOG(ERROR) << key << " - " << got << " is not supported...";
    return false;
  }
  return true;
}

static PropertyType StringToPropertyType(const std::string& str) {
  if (str == "int32") {
    return PropertyType::kInt32;
  } else if (str == "Date") {
    return PropertyType::kDate;
  } else if (str == "String") {
    return PropertyType::kString;
  } else if (str == "Empty") {
    return PropertyType::kEmpty;
  } else if (str == "int64") {
    return PropertyType::kInt64;
  } else {
    return PropertyType::kEmpty;
  }
}

EdgeStrategy StringToEdgeStrategy(const std::string& str) {
  if (str == "None") {
    return EdgeStrategy::kNone;
  } else if (str == "Single") {
    return EdgeStrategy::kSingle;
  } else if (str == "Multiple") {
    return EdgeStrategy::kMultiple;
  } else {
    return EdgeStrategy::kMultiple;
  }
}

StorageStrategy StringToStorageStrategy(const std::string& str) {
  if (str == "None") {
    return StorageStrategy::kNone;
  } else if (str == "Mem") {
    return StorageStrategy::kMem;
  } else {
    return StorageStrategy::kMem;
  }
}

static bool parse_vertex_properties(YAML::Node node,
                                    const std::string& label_name,
                                    std::vector<PropertyType>& types,
                                    std::vector<StorageStrategy>& strategies) {
  if (!node || !node.IsSequence()) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  int prop_num = node.size();
  if (prop_num == 0) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  if (!expect_config(node[0], "name", "_ID") ||
      !expect_config(node[0], "type", "int64")) {
    LOG(ERROR) << "the first property of vertex-" << label_name
               << " should be _ID with type int64";
    return false;
  }

  for (int i = 1; i < prop_num; ++i) {
    std::string prop_type_str, strategy_str;
    if (!get_scalar(node[i], "type", prop_type_str)) {
      LOG(ERROR) << "type of vertex-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    get_scalar(node[i], "storage_strategy", strategy_str);
    types.push_back(StringToPropertyType(prop_type_str));
    strategies.push_back(StringToStorageStrategy(strategy_str));
  }

  return true;
}

static bool parse_vertex_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::pair<std::string, std::string>>& files) {
  std::string label_name;
  if (!get_scalar(node, "label_name", label_name)) {
    return false;
  }
  size_t max_num = ((size_t) 1) << 32;
  get_scalar(node, "max_vertex_num", max_num);
  std::vector<PropertyType> property_types;
  std::vector<StorageStrategy> strategies;
  if (!parse_vertex_properties(node["properties"], label_name, property_types,
                               strategies)) {
    return false;
  }
  schema.add_vertex_label(label_name, property_types, strategies, max_num);
  std::vector<std::string> files_got;
  get_sequence(node, "files", files_got);
  for (auto& f : files_got) {
    if (f[0] == '/') {
      files.emplace_back(label_name, f);
    } else {
      files.emplace_back(label_name, prefix + f);
    }
  }

  return true;
}

static bool parse_vertices_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::pair<std::string, std::string>>& files) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "vertex is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_vertex_schema(node[i], schema, prefix, files)) {
      return false;
    }
  }
  return true;
}

static bool parse_edge_properties(YAML::Node node,
                                  const std::string& label_name,
                                  std::vector<PropertyType>& types) {
  if (!node || !node.IsSequence()) {
    LOG(ERROR) << "properties of vertex-" << label_name
               << " not set properly... ";
    return false;
  }

  int prop_num = node.size();
  if (prop_num <= 1) {
    LOG(ERROR) << "properties of edge-" << label_name
               << " not set properly... ";
    return false;
  }

  if (!expect_config(node[0], "name", "_SRC") ||
      !expect_config(node[0], "type", "int64")) {
    LOG(ERROR) << "the first property of edge-" << label_name
               << " should be _SRC with type int64";
    return false;
  }
  if (!expect_config(node[1], "name", "_DST") ||
      !expect_config(node[0], "type", "int64")) {
    LOG(ERROR) << "the second property of edge-" << label_name
               << " should be _DST with type int64";
    return false;
  }

  for (int i = 2; i < prop_num; ++i) {
    std::string prop_type_str, strategy_str;
    if (!get_scalar(node[i], "type", prop_type_str)) {
      LOG(ERROR) << "type of edge-" << label_name << " prop-" << i - 1
                 << " is not specified...";
      return false;
    }
    types.push_back(StringToPropertyType(prop_type_str));
  }

  return true;
}

static bool parse_edge_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::tuple<std::string, std::string, std::string, std::string>>&
    files) {
  std::string src_label_name, dst_label_name, edge_label_name;
  if (!get_scalar(node, "src_label_name", src_label_name)) {
    return false;
  }
  if (!get_scalar(node, "dst_label_name", dst_label_name)) {
    return false;
  }
  if (!get_scalar(node, "edge_label_name", edge_label_name)) {
    return false;
  }
  std::vector<PropertyType> property_types;
  if (!parse_edge_properties(node["properties"], edge_label_name,
                             property_types)) {
    return false;
  }
  EdgeStrategy ie = EdgeStrategy::kMultiple;
  EdgeStrategy oe = EdgeStrategy::kMultiple;
  std::string ie_str, oe_str;
  if (get_scalar(node, "outgoing_edge_strategy", oe_str)) {
    oe = StringToEdgeStrategy(oe_str);
  }
  if (get_scalar(node, "incoming_edge_strategy", ie_str)) {
    ie = StringToEdgeStrategy(ie_str);
  }
  schema.add_edge_label(src_label_name, dst_label_name, edge_label_name,
                        property_types, oe, ie);
  std::vector<std::string> files_got;
  get_sequence(node, "files", files_got);
  for (auto& f : files_got) {
    if (f[0] == '/') {
      files.emplace_back(src_label_name, dst_label_name, edge_label_name, f);
    } else {
      files.emplace_back(src_label_name, dst_label_name, edge_label_name,
                         prefix + f);
    }
  }

  return true;
}

static bool parse_edges_schema(
    YAML::Node node, Schema& schema, const std::string& prefix,
    std::vector<std::tuple<std::string, std::string, std::string, std::string>>&
    files) {
  if (!node.IsSequence()) {
    LOG(ERROR) << "edge is not set properly";
    return false;
  }
  int num = node.size();
  for (int i = 0; i < num; ++i) {
    if (!parse_edge_schema(node[i], schema, prefix, files)) {
      return false;
    }
  }

  return true;
}

static bool parse_config_file(
    const std::string& path, Schema& schema,
    std::vector<std::string>& stored_procedures,
    std::vector<std::pair<std::string, std::string>>& vertex_files,
    std::vector<std::tuple<std::string, std::string, std::string, std::string>>&
    edge_files) {
  YAML::Node root = YAML::LoadFile(path);
  YAML::Node graph_node = root["graph"];
  if (!graph_node || !graph_node.IsMap()) {
    LOG(ERROR) << "graph is not set properly";
    return false;
  }
  if (!expect_config(graph_node, "file_format", "standard_csv")) {
    return false;
  }
  if (!expect_config(graph_node, "graph_store", "mutable_csr")) {
    return false;
  }

  if (!graph_node["vertex"]) {
    LOG(ERROR) << "vertex is not set";
    return false;
  }

  std::string graph_data_prefix;
  get_scalar(root, "graph_dir", graph_data_prefix);
  if (!graph_data_prefix.empty()) {
    if (*graph_data_prefix.rbegin() != '/') {
      graph_data_prefix += '/';
    }
  }

  if (!parse_vertices_schema(graph_node["vertex"], schema, graph_data_prefix,
                             vertex_files)) {
    return false;
  }

  if (graph_node["edge"]) {
    if (!parse_edges_schema(graph_node["edge"], schema, graph_data_prefix,
                            edge_files)) {
      return false;
    }
  }

  if (root["stored_procedures"]) {
    std::string stored_procedures_prefix;
    get_scalar(root, "stored_procedures_dir", stored_procedures_prefix);
    if (!stored_procedures_prefix.empty()) {
      if (*stored_procedures_prefix.rbegin() != '/') {
        stored_procedures_prefix += '/';
      }
    }
    std::vector<std::string> files_got;
    if (!get_sequence(root, "stored_procedures", files_got)) {
      LOG(ERROR) << "stored_procedures is not set properly";
    }
    for (auto& f : files_got) {
      std::string path;
      if (f[0] == '/') {
        path = f;
      } else {
        path = stored_procedures_prefix + f;
      }
      if (!std::filesystem::exists(path)) {
        LOG(ERROR) << "plugin - " << path << " file not found...";
        return false;
      }
      stored_procedures.push_back(std::filesystem::canonical(path));
    }
  }

  return true;
}

}  // namespace config_parsing

std::tuple<Schema,
    std::vector<std::pair<std::string, std::string>>,
    std::vector<std::tuple<std::string, std::string, std::string, std::string>>,
    std::vector<std::string>>
Schema::LoadFromYaml(const std::string& path) {
  Schema schema;
  std::vector<std::pair<std::string, std::string>> vertex_files;
  std::vector<std::tuple<std::string, std::string, std::string, std::string>>
      edge_files;
  std::vector<std::string> plugins;

  config_parsing::parse_config_file(path, schema, plugins, vertex_files,
                                    edge_files);

  return std::make_tuple(schema, vertex_files, edge_files, plugins);
}


}  // namespace gs
