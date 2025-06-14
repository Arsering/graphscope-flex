# 创建一个简单的测试程序
cat > test_io_uring.c <<EOF
#include <liburing.h>
#include <stdio.h>

int main() {
    struct io_uring ring;
    int ret = io_uring_queue_init(8, &ring, 0);
    if (ret < 0) {
        perror("io_uring_queue_init");
        return 1;
    }
    printf("IO_URing initialized successfully\n");
    io_uring_queue_exit(&ring);
    return 0;
}
EOF

# 安装liburing开发库
sudo apt-get install liburing-dev  # Debian/Ubuntu
# 或
sudo yum install liburing-devel    # CentOS/RHEL

# 编译测试程序
gcc -o test_io_uring test_io_uring.c -luring

# 用Valgrind运行测试程序
valgrind ./test_io_uring
