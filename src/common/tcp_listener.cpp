#include <tinyrpc/common/tcp_listener.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <utility>

namespace tinyrpc {

TcpListener::TcpListener(int fd) noexcept
    : fd_(fd) {}

TcpListener::~TcpListener() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

TcpListener::TcpListener(TcpListener&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)) {}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

std::optional<TcpListener> TcpListener::bind(const Endpoint& endpoint) {
    // 创建监听 socket
    const auto listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        return std::nullopt;
    }

    TcpListener listener(listen_fd);

    // 允许端口复用
    int opt = 1;
    if (setsockopt(listen_fd,
                   SOL_SOCKET,
                   SO_REUSEADDR,
                   &opt,
                   sizeof(opt)) < 0) {
        return std::nullopt;
    }

    // 绑定监听地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(endpoint.port);

    if (inet_pton(AF_INET, endpoint.ip.c_str(), &addr.sin_addr) <= 0) {
        return std::nullopt;
    }

    if (::bind(listen_fd,
               reinterpret_cast<sockaddr*>(&addr),
               sizeof(addr)) < 0) {
        return std::nullopt;
    }

    // 开始监听
    if (listen(listen_fd, 16) < 0) {
        return std::nullopt;
    }

    return listener;
}

std::optional<TcpSocket> TcpListener::accept() const {
    if (fd_ < 0) {
        return std::nullopt;
    }

    const auto client_fd = ::accept(fd_, nullptr, nullptr);
    if (client_fd < 0) {
        return std::nullopt;
    }

    TcpSocket client_socket(client_fd);
    if (!client_socket.setTimeout()) {
        return std::nullopt;
    }

    return client_socket;
}

// 返回借用的文件描述符，关闭操作仍由 TcpListener 负责
int TcpListener::fd() const noexcept {
    return fd_;
}

} // namespace tinyrpc
