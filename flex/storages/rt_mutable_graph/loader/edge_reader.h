#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <fstream>
#include <sstream>
#include <stdexcept>

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
        const std::vector<Edge>& edges,std::unordered_map<int64_t, int64_t>* message_to_person_map);

    void build_comment_to_person_map(
        const std::string& file_path,std::unordered_map<int64_t, int64_t>* message_to_person_map);
};