#include <cstddef>
#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <thread>
#include <random>
#include <chrono>
#include <atomic>
#include <sys/types.h>
#include <sys/stat.h>
#include <cstring>
#include <atomic>

// 文件路径和大小
const char *FILE_PATH1 = "/data-1/yichengzhang/data/experiment_space/LDBC_SNB-nvme/nvme/testfile";
const char *FILE_PATH2 = "/data-1/yichengzhang/data/experiment_space/LDBC_SNB-nvme/nvme/testfile2";
const size_t FILE_SIZE = 50L * 1024 * 1024 * 1024; // 50GB
const size_t ALIGNMENT = 4096; // 4KB对齐
const size_t PAGE_SIZE = 4096; // 4KB

// 测试参数
const size_t THREAD_COUNT = 30;
const size_t BLOCK_SIZE_1 = 20 * 1024; // 20KB
const size_t BLOCK_SIZE_2_1 = 12 * 1024; // 12KB
const size_t BLOCK_SIZE_2_2 = 8 * 1024; // 8KB
const size_t OFFSET_DIFF = 1L * 1024 * 1024 * 1024; // 1GB
const size_t TEST_CYCLES=10000;

// 统计延迟的原子变量
std::atomic<size_t> latency_sum_1{0};
std::atomic<size_t> latency_sum_2{0};

size_t GetSystemTime() {
  size_t hi, lo;
  __asm__ __volatile__("" : : : "memory");
  __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi));
  __asm__ __volatile__("" : : : "memory");
  return ((size_t) lo) | (((size_t) hi) << 32);
}

// 初始化文件并写入页号
void initialize_file(const std::string &file_name) {
    int fd = open(file_name.c_str(), O_CREAT | O_RDWR | O_DIRECT, 0666);
    if (fd < 0) {
        perror("Failed to open file");
        exit(1);
    }

    std::cout << "Initializing file with size " << FILE_SIZE / (1024 * 1024 * 1024) << "GB...\n";
    char *buffer = static_cast<char *>(aligned_alloc(ALIGNMENT, PAGE_SIZE));
    if (!buffer) {
        perror("Failed to allocate aligned memory");
        close(fd);
        exit(1);
    }

    for (size_t page_num = 0; page_num < FILE_SIZE / PAGE_SIZE; ++page_num) {
        memset(buffer, 0, PAGE_SIZE);
        std::memcpy(buffer, &page_num, sizeof(page_num)); // 在页开头写入页号
        ssize_t written = pwrite(fd, buffer, PAGE_SIZE, page_num * PAGE_SIZE);
        if (written != PAGE_SIZE) {
            perror("Failed to write to file");
            free(buffer);
            close(fd);
            exit(1);
        }
    }

    free(buffer);
    close(fd);
    std::cout << "File initialization complete.\n";
}

// 校验页号的辅助函数
bool verify_page_number(const char *buffer, size_t expected_page_num) {
    size_t page_num;
    std::memcpy(&page_num, buffer, sizeof(page_num));
    return page_num == expected_page_num;
}

// 读取测试方式1
void test_method_1(int fd) {
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, FILE_SIZE - BLOCK_SIZE_1);

    for (int i = 0; i < TEST_CYCLES; ++i) { // 每线程测试100次
        size_t offset = dist(rng) & ~(ALIGNMENT - 1); // 4KB对齐
        char buffer[BLOCK_SIZE_1] __attribute__((aligned(ALIGNMENT)));

        auto start = std::chrono::high_resolution_clock::now();
        pread(fd, buffer, BLOCK_SIZE_1, offset);
        auto end = std::chrono::high_resolution_clock::now();

    // 校验读取的页号
        size_t page_num = offset / PAGE_SIZE;
        if (!verify_page_number(buffer, page_num)) {
            std::cerr << "Page number mismatch at offset " << offset << ": expected " << page_num << "\n";
        }

        size_t latency = std::chrono::duration<double, std::micro>(end - start).count();
        latency_sum_1.fetch_add(latency, std::memory_order_relaxed);
    }
}

// 读取测试方式2
void test_method_2(int fd) {
    std::mt19937_64 rng(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, FILE_SIZE - OFFSET_DIFF - BLOCK_SIZE_2_2);

    for (int i = 0; i < TEST_CYCLES; ++i) { // 每线程测试100次
        size_t offset1 = dist(rng) & ~(ALIGNMENT - 1); // 4KB对齐
        size_t offset2 = (offset1 + OFFSET_DIFF) & ~(ALIGNMENT - 1);
        char buffer1[BLOCK_SIZE_2_1] __attribute__((aligned(ALIGNMENT)));
        char buffer2[BLOCK_SIZE_2_2] __attribute__((aligned(ALIGNMENT)));

        auto start = std::chrono::high_resolution_clock::now();
        pread(fd, buffer1, BLOCK_SIZE_2_1, offset1);
        pread(fd, buffer2, BLOCK_SIZE_2_2, offset2);
        auto end = std::chrono::high_resolution_clock::now();

        // 校验读取的页号
        size_t page_num1 = offset1 / PAGE_SIZE;
        size_t page_num2 = offset2 / PAGE_SIZE;
        if (!verify_page_number(buffer1, page_num1)) {
            std::cerr << "Page number mismatch at offset " << offset1 << ": expected " << page_num1 << "\n";
        }
        if (!verify_page_number(buffer2, page_num2)) {
            std::cerr << "Page number mismatch at offset " << offset2 << ": expected " << page_num2 << "\n";
        }

        size_t latency = std::chrono::duration<double, std::micro>(end - start).count();
        latency_sum_2.fetch_add(latency, std::memory_order_relaxed);
    }
}

int main() {
    // 初始化文件
    std::cout << "Initializing file..." << std::endl;
    // initialize_file(FILE_PATH1);
    // initialize_file(FILE_PATH2);
    std::cout << "File initialized." << std::endl;

    // 打开文件
    int fd1 = open(FILE_PATH1, O_RDONLY | O_DIRECT);
    if (fd1 == -1) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }
    int fd2 = open(FILE_PATH2, O_RDONLY | O_DIRECT);
    if (fd2 == -1) {
        perror("Failed to open file");
        exit(EXIT_FAILURE);
    }

    // 启动测试
    std::vector<std::thread> threads;
    auto start = std::chrono::high_resolution_clock::now();

    // 测试方式1
    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back(test_method_1, fd1);
    }
    for (auto &t : threads) t.join();
    threads.clear();
    double avg_latency_1 = latency_sum_1 / (THREAD_COUNT * TEST_CYCLES);

    // 测试方式2
    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back(test_method_2, fd2);
    }
    for (auto &t : threads) t.join();
    double avg_latency_2 = latency_sum_2 / (THREAD_COUNT * TEST_CYCLES);

    auto end = std::chrono::high_resolution_clock::now();
    close(fd1);
    close(fd2);

    // 输出结果
    std::cout << "Average latency for method 1: " << avg_latency_1 << " µs" << std::endl;
    std::cout << "Average latency for method 2: " << avg_latency_2 << " µs" << std::endl;

    std::cout << "Test completed in "
              << std::chrono::duration<double>(end - start).count()
              << " seconds." << std::endl;

    return 0;
}
