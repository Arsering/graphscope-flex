#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include "flex/storages/rt_mutable_graph/loader/basic_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loader/i_fragment_loader.h"
#include "flex/storages/rt_mutable_graph/loading_config.h"
#include "flex/storages/rt_mutable_graph/mutable_property_fragment.h"

#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include "arrow/util/value_parsing.h"

#include "grape/util.h"
#include "flex/storages/rt_mutable_graph/loader/edge_reader.h"

struct Edge {
    int64_t comment_id;
    int64_t person_id;
};

class EdgeReader {
public:
    // 从CSV文件读取边信息
    static std::vector<Edge> read_edges_from_csv(const std::string& file_path);
    
    // 构建comment_id到person_id的映射
    void build_comment_to_person_map(
        const std::vector<Edge>& edges,std::unordered_map<int64_t, int64_t>* message_to_person_map,std::unordered_map<int64_t, int64_t>* person_degree_map);

    void build_comment_to_person_map(
        const std::string& file_path,std::unordered_map<int64_t, int64_t>* message_to_person_map,std::unordered_map<int64_t, int64_t>* person_degree_map);
};