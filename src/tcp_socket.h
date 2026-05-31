#pragma once

#include <cstdint>
#include <string>

namespace tinyrpc {

// TCP 基础工具
class TcpSocket {
public:
    static int connectToServer(const std::string& ip, uint16_t port);

    static int createServerSocket(const std::string& ip, uint16_t port);

    static bool sendAll(int fd, const std::string& data);

    static bool sendAll(int fd, const char* data, size_t len);

    static bool recvAll(int fd, char* data, size_t len);

    static bool recvAll(int fd, std::string& out, size_t len);
};

} // namespace tinyrpc
