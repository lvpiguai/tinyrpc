#pragma once

#include <cstdint>
#include <string>

namespace tinyrpc {

// 网络端点
struct Endpoint {
    std::string ip;
    uint16_t port = 0;

    bool operator==(const Endpoint& other) const {
        return ip == other.ip && port == other.port;
    }
};

} // namespace tinyrpc
