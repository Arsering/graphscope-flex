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
