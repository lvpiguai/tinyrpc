#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <sys/types.h>

namespace tinyrpc {

// 拥有一个 TCP 连接，析构时自动关闭
class TcpSocket {
public:
    static constexpr int kDefaultTimeoutMs = 5000;

    // 接管已有文件描述符的所有权
    explicit TcpSocket(int fd) noexcept;

    ~TcpSocket();

    TcpSocket(const TcpSocket&) = delete;
    TcpSocket& operator=(const TcpSocket&) = delete;

    TcpSocket(TcpSocket&& other) noexcept;
    TcpSocket& operator=(TcpSocket&& other) noexcept;

    // 在指定超时时间内连接远程服务器
    static std::optional<TcpSocket> connect(
        const std::string& ip,
        uint16_t port,
        int timeout_ms = kDefaultTimeoutMs);

    // 设置收发超时
    bool setTimeout(int timeout_ms = kDefaultTimeoutMs);

    // 设置阻塞或非阻塞模式
    bool setNonBlocking(bool enabled = true);

    // 获取底层文件描述符，不转移所有权
    int fd() const noexcept;

    // 发送全部数据
    bool sendAll(const std::string& data);
    bool sendAll(const char* data, size_t len);

    // 发送当前能够写入的数据，返回值沿用 send 语义
    ssize_t sendSome(const char* data, size_t len);

    // 接收当前可读的数据，返回值沿用 recv 语义
    ssize_t recvSome(char* data, size_t len);

    // 接收指定长度的数据
    bool recvAll(char* data, size_t len);
    bool recvAll(std::string& out, size_t len);

    // 接收不超过指定长度的一行文本
    std::optional<std::string> recvLine(size_t max_size);

private:
    int fd_ = -1;
};

} // namespace tinyrpc
