#include <tinyrpc/common/tcp_socket.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <utility>

namespace tinyrpc {

TcpSocket::TcpSocket(int fd) noexcept
    : fd_(fd) {}

TcpSocket::~TcpSocket() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

TcpSocket::TcpSocket(TcpSocket&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)) {}

TcpSocket& TcpSocket::operator=(TcpSocket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

std::optional<TcpSocket> TcpSocket::connect(const std::string& ip, uint16_t port) {
    // 创建 TCP socket
    const auto fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }

    TcpSocket socket(fd);

    // 填写服务端地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    // 转换 IP 地址
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        return std::nullopt;
    }

    // 发起连接
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        return std::nullopt;
    }

    if (!socket.setTimeout()) {
        return std::nullopt;
    }

    return socket;
}

bool TcpSocket::setTimeout(int timeout_ms) {
    if (fd_ < 0) {
        return false;
    }

    if (timeout_ms <= 0) {
        return false;
    }

    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    return setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0 &&
           setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0;
}

bool TcpSocket::sendAll(const std::string& data) {
    // 转给 buffer 版本发送
    return sendAll(data.data(), data.size());
}

bool TcpSocket::sendAll(const char* data, size_t len) {
    if (fd_ < 0) {
        return false;
    }

    // 循环发送到指定长度
    size_t sent = 0;

    while (sent < len) {
        const auto n = send(fd_, data + sent, len - sent, 0);
        if (n <= 0) {
            return false;
        }
        sent += static_cast<size_t>(n);
    }

    return true;
}

bool TcpSocket::recvAll(char* data, size_t len) {
    if (fd_ < 0) {
        return false;
    }

    // 循环接收到指定长度
    size_t received = 0;

    while (received < len) {
        const auto n = recv(fd_, data + received, len - received, 0);
        if (n <= 0) {
            return false;
        }
        received += static_cast<size_t>(n);
    }

    return true;
}

bool TcpSocket::recvAll(std::string& out, size_t len) {
    // 扩容后直接接收
    out.resize(len);
    return recvAll(out.data(), len);
}

std::optional<std::string> TcpSocket::recvLine(size_t max_size) {
    if (fd_ < 0) {
        return std::nullopt;
    }

    std::string line;

    char ch = '\0';
    while (true) {
        const auto n = recv(fd_, &ch, 1, 0);
        if (n <= 0) {
            return std::nullopt;
        }

        if (ch == '\n') {
            return line;
        }

        if (ch != '\r') {
            if (line.size() >= max_size) {
                return std::nullopt;
            }
            line.push_back(ch);
        }
    }
}

} // namespace tinyrpc
