#pragma once

#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/common/tcp_socket.h>

#include <cstdint>
#include <optional>
#include <string>

namespace tinyrpc {

// 拥有一个 TCP 监听端口，析构时自动关闭
class TcpListener {
public:
    ~TcpListener();

    TcpListener(const TcpListener&) = delete;
    TcpListener& operator=(const TcpListener&) = delete;

    TcpListener(TcpListener&& other) noexcept;
    TcpListener& operator=(TcpListener&& other) noexcept;

    // 绑定地址并开始监听
    static std::optional<TcpListener> bind(const Endpoint& endpoint);

    // 接收客户端连接并设置默认收发超时
    std::optional<TcpSocket> accept() const;

    // 获取底层文件描述符，不转移所有权
    int fd() const noexcept;

private:
    explicit TcpListener(int fd) noexcept;

    int fd_ = -1;
};

} // namespace tinyrpc
