#pragma once

#include <cstdint>
#include <string>

namespace tinyrpc {

// 网络端点
struct Endpoint {
    std::string ip;
    uint16_t port = 0;
};

} // namespace tinyrpc
