#include "flex/utils/access_logger.h"

namespace gs {

static thread_local ThreadLog* access_logger_g = nullptr;

void set_thread_logger(ThreadLog* access_logger) {
  access_logger_g = access_logger;
}
ThreadLog* get_thread_logger() { return access_logger_g; }

}  // namespace gs
