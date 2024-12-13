#pragma once
#include <bits/stdint-intn.h>
#include <mutex>
#include <unordered_map>
#include <atomic>
#include <string>
#include <fstream>
#include <type_traits>
#include <iostream>
#include <iomanip>
#include "base_indexer.h"

// 泛型类，OID是外部ID类型，LID是本地ID类型
template<typename OID = int64_t, typename LID=int64_t>
class MessageIdAllocator : public gs::BaseIndexer<LID> {
    // 确保OID和LID是整数类型
    static_assert(std::is_integral<OID>::value, "OID must be an integral type");
    static_assert(std::is_integral<LID>::value, "LID must be an integral type");

private:
    static constexpr int MESSAGE_IN_BLOCK = 3;  // 每个block中可以存放的消息数量
    
    mutable std::mutex mtx;  // 用于保护map的互斥锁
    std::unordered_map<OID, int> person_counters;  // 每个person的当前计数器
    std::atomic<int> global_block_count{0};  // 全局block计数器
    
    // 存储每个message id对应的person
    std::unordered_map<LID, OID> message_owners;
    
    // 存储外部消息ID到内部消息ID的映射
    std::unordered_map<OID, LID> oid_to_lid_map;

    // 存储内部消息ID到外部消息ID的映射
    std::unordered_map<LID, OID> lid_to_oid_map;

public:
    MessageIdAllocator() = default;
    
    // 获取下一个消息ID，现在需要传入外部消息ID
    LID get_message_id(OID person, OID message_oid) {
        std::lock_guard<std::mutex> lock(mtx);
        
        // 检查这个oid是否已经分配过lid
        auto oid_it = oid_to_lid_map.find(message_oid);
        if (oid_it != oid_to_lid_map.end()) {
            return oid_it->second;  // 如果已经分配过，直接返回对应的lid
        }
        
        // 获取该person当前的计数器值
        auto it = person_counters.find(person);
        if (it == person_counters.end()) {
            // 新的person，分配一个新的block
            person_counters[person] = global_block_count.fetch_add(1) * MESSAGE_IN_BLOCK;
            it = person_counters.find(person);
        }
        
        // 计算当前消息的全局offset
        LID current_offset = static_cast<LID>(it->second);
        
        // 记录message id的所有者
        message_owners[current_offset] = person;
        
        // 记录oid到lid的映射
        oid_to_lid_map[message_oid] = current_offset;
        lid_to_oid_map[current_offset] = message_oid;
        
        // 增加计数器
        it->second++;
        
        // 检查是否需要分配新的block
        if (it->second % MESSAGE_IN_BLOCK == 0) {
            // 分配新的block
            it->second = global_block_count.fetch_add(1) * MESSAGE_IN_BLOCK;
        }
        
        return current_offset;
    }
    
    // 根据外部消息ID获取内部消息ID
    bool get_lid_by_oid(OID message_oid, LID& lid) const {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = oid_to_lid_map.find(message_oid);
        if (it == oid_to_lid_map.end()) {
            return false;
        }
        lid = it->second;
        return true;
    }
    
    // 根据内部消息ID获取外部消息ID
    bool get_oid_by_lid(LID message_lid, OID& oid) const {
        std::lock_guard<std::mutex> lock(mtx);
        auto it = lid_to_oid_map.find(message_lid);
        if (it == lid_to_oid_map.end()) {
            return false;
        }
        oid = it->second;
        return true;
    }
    
    // 获取当前全局block数量
    int get_block_count() const { return global_block_count.load(); }
    
    // 打印block分配情况
    void print_block_allocation() const {
        std::lock_guard<std::mutex> lock(mtx);
        
        std::cout << "\n=== Block Allocation Status ===\n";
        
        int total_blocks = get_block_count();
        for (int block = 0; block < total_blocks; ++block) {
            LID block_start = static_cast<LID>(block * MESSAGE_IN_BLOCK);
            std::cout << "\nBlock " << block << " (Message IDs " 
                     << block_start << " - " << (block_start + MESSAGE_IN_BLOCK - 1) << "):\n";
            
            // 检查这个block中的每个message id
            for (int offset = 0; offset < MESSAGE_IN_BLOCK; ++offset) {
                LID msg_id = block_start + offset;
                auto it = message_owners.find(msg_id);
                if (it != message_owners.end()) {
                    std::cout << "  Message ID " << std::setw(3) << msg_id 
                             << " -> Person " << it->second;
                    
                    // 尝试找到对应的OID
                    auto oid_it = lid_to_oid_map.find(msg_id);
                    if (oid_it != lid_to_oid_map.end()) {
                        std::cout << " (OID: " << oid_it->second << ")";
                    }
                    std::cout << "\n";
                } else {
                    std::cout << "  Message ID " << std::setw(3) << msg_id 
                             << " -> Unallocated\n";
                }
            }
        }
        std::cout << "\nTotal Blocks: " << total_blocks << "\n";
        std::cout << "=========================\n";
    }
        // 将当前状态保存到文件
    void dump(const std::string& file_path) const {
        std::lock_guard<std::mutex> lock(mtx);
        std::ofstream file(file_path, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open file for writing: " + file_path);
        }

        // 保存 block 数量
        int block_count = global_block_count.load();
        file.write(reinterpret_cast<const char*>(&block_count), sizeof(block_count));

        // 保存 person_counters
        size_t counter_size = person_counters.size();
        file.write(reinterpret_cast<const char*>(&counter_size), sizeof(counter_size));
        for (const auto& pair : person_counters) {
            file.write(reinterpret_cast<const char*>(&pair.first), sizeof(pair.first));
            file.write(reinterpret_cast<const char*>(&pair.second), sizeof(pair.second));
        }

        // 保存 message_owners
        size_t owners_size = message_owners.size();
        file.write(reinterpret_cast<const char*>(&owners_size), sizeof(owners_size));
        for (const auto& pair : message_owners) {
            file.write(reinterpret_cast<const char*>(&pair.first), sizeof(pair.first));
            file.write(reinterpret_cast<const char*>(&pair.second), sizeof(pair.second));
        }

        // 保存 oid_to_lid_map
        size_t oid_map_size = oid_to_lid_map.size();
        file.write(reinterpret_cast<const char*>(&oid_map_size), sizeof(oid_map_size));
        for (const auto& pair : oid_to_lid_map) {
            file.write(reinterpret_cast<const char*>(&pair.first), sizeof(pair.first));
            file.write(reinterpret_cast<const char*>(&pair.second), sizeof(pair.second));
        }

        // 保存 lid_to_oid_map
        size_t lid_map_size = lid_to_oid_map.size();
        file.write(reinterpret_cast<const char*>(&lid_map_size), sizeof(lid_map_size));
        for (const auto& pair : lid_to_oid_map) {
            file.write(reinterpret_cast<const char*>(&pair.first), sizeof(pair.first));
            file.write(reinterpret_cast<const char*>(&pair.second), sizeof(pair.second));
        }
    }

    // 从文件加载状态
    void load(const std::string& file_path) {
        std::lock_guard<std::mutex> lock(mtx);
        std::ifstream file(file_path, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open file for reading: " + file_path);
        }

        // 清除现有状态
        person_counters.clear();
        message_owners.clear();
        oid_to_lid_map.clear();
        lid_to_oid_map.clear();

        // 加载 block 数量
        int block_count;
        file.read(reinterpret_cast<char*>(&block_count), sizeof(block_count));
        global_block_count.store(block_count);

        // 加载 person_counters
        size_t counter_size;
        file.read(reinterpret_cast<char*>(&counter_size), sizeof(counter_size));
        for (size_t i = 0; i < counter_size; ++i) {
            OID first;
            int second;
            file.read(reinterpret_cast<char*>(&first), sizeof(first));
            file.read(reinterpret_cast<char*>(&second), sizeof(second));
            person_counters[first] = second;
        }

        // 加载 message_owners
        size_t owners_size;
        file.read(reinterpret_cast<char*>(&owners_size), sizeof(owners_size));
        for (size_t i = 0; i < owners_size; ++i) {
            LID first;
            OID second;
            file.read(reinterpret_cast<char*>(&first), sizeof(first));
            file.read(reinterpret_cast<char*>(&second), sizeof(second));
            message_owners[first] = second;
        }

        // 加载 oid_to_lid_map
        size_t oid_map_size;
        file.read(reinterpret_cast<char*>(&oid_map_size), sizeof(oid_map_size));
        for (size_t i = 0; i < oid_map_size; ++i) {
            OID first;
            LID second;
            file.read(reinterpret_cast<char*>(&first), sizeof(first));
            file.read(reinterpret_cast<char*>(&second), sizeof(second));
            oid_to_lid_map[first] = second;
        }

        // 加载 lid_to_oid_map
        size_t lid_map_size;
        file.read(reinterpret_cast<char*>(&lid_map_size), sizeof(lid_map_size));
        for (size_t i = 0; i < lid_map_size; ++i) {
            LID first;
            OID second;
            file.read(reinterpret_cast<char*>(&first), sizeof(first));
            file.read(reinterpret_cast<char*>(&second), sizeof(second));
            lid_to_oid_map[first] = second;
        }
    }

    // 实现基类接口
    bool get_index(OID oid, LID& lid) const override {
        return get_lid_by_oid(oid, lid);
    }
    
    OID get_key(const LID& lid) const override {
        OID oid;
        if(!get_oid_by_lid(lid, oid)) {
            throw std::runtime_error("LID not found");
        }
        return oid;
    }
    
    size_t size() const override {
        return global_block_count.load() * MESSAGE_IN_BLOCK;
    }

    void dump(const std::string& name, const std::string& snapshot_dir) override {
        dump(snapshot_dir + "/" + name);
    }

    // 添加缺失的虚函数实现
    void open(const std::string& name, const std::string& snapshot_dir,
              const std::string& work_dir) override {
        load(snapshot_dir + "/" + name);
    }
};

