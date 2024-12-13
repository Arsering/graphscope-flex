#include "flex/storages/rt_mutable_graph/loader/edge_reader.h"

std::vector<Edge> EdgeReader::read_edges_from_csv(const std::string& file_path) {
    std::vector<Edge> edges;
    std::ifstream file(file_path);
    
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + file_path);
    }
    
    std::string line;
    // 跳过标题行
    std::getline(file, line);
    
    // 读取数据行
    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string comment_id_str, person_id_str;
        
        // 读取comment_id，使用'|'作为分隔符
        if (!std::getline(ss, comment_id_str, '|')) {
            continue;
        }
        
        // 读取person_id
        if (!std::getline(ss, person_id_str, '|')) {
            continue;
        }
        
        try {
            Edge edge;
            edge.comment_id = std::stoll(comment_id_str);
            edge.person_id = std::stoll(person_id_str);
            edges.push_back(edge);
        } catch (const std::exception& e) {
            // 跳过无效的行
            continue;
        }
    }
    
    return edges;
}

std::unordered_map<int64_t, int64_t> EdgeReader::build_comment_to_person_map(
    const std::vector<Edge>& edges) {
    std::unordered_map<int64_t, int64_t> comment_to_person;
    
    for (const auto& edge : edges) {
        comment_to_person[edge.comment_id] = edge.person_id;
    }
    
    return comment_to_person;
} 