#include <tinyrpc/common/tcp_socket.h>

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <utility>

namespace tinyrpc {

// 接管已有 socket 文件描述符
TcpSocket::TcpSocket(int fd) noexcept
    : fd_(fd) {}

// 关闭当前持有的 socket
TcpSocket::~TcpSocket() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

// 转移 socket 所有权
TcpSocket::TcpSocket(TcpSocket&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)) {}

// 关闭旧 socket 后接管新所有权
TcpSocket& TcpSocket::operator=(TcpSocket&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) {
            close(fd_);
        }
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

// 在指定时间内建立 TCP 连接
std::optional<TcpSocket> TcpSocket::connect(const Endpoint& endpoint,
                                            int timeout_ms) {
    if (timeout_ms <= 0) {
        errno = EINVAL;
        return std::nullopt;
    }

    // 创建 TCP socket
    const auto fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return std::nullopt;
    }

    TcpSocket socket(fd);

    // 填写服务端地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(endpoint.port);

    // 转换 IP 地址
    if (inet_pton(AF_INET, endpoint.ip.c_str(), &addr.sin_addr) <= 0) {
        return std::nullopt;
    }

    // 非阻塞 connect 才能限制连接建立阶段的等待时间
    if (!socket.setNonBlocking()) {
        return std::nullopt;
    }

    const auto connected =
        ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (connected < 0 && errno != EINPROGRESS) {
        return std::nullopt;
    }

    if (connected < 0) {
        pollfd event{fd, POLLOUT, 0};
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);

        while (true) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                errno = ETIMEDOUT;
                return std::nullopt;
            }

            auto remaining_ms = std::chrono::duration_cast<
                std::chrono::milliseconds>(deadline - now).count();
            if (remaining_ms == 0) {
                remaining_ms = 1;
            }

            const auto ready = poll(&event, 1,
                                    static_cast<int>(remaining_ms));
            if (ready == 0) {
                errno = ETIMEDOUT;
                return std::nullopt;
            }
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return std::nullopt;
            }

            int socket_error = 0;
            socklen_t error_size = sizeof(socket_error);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR,
                           &socket_error, &error_size) < 0) {
                return std::nullopt;
            }
            if (socket_error != 0) {
                errno = socket_error;
                return std::nullopt;
            }
            break;
        }
    }

    // 建连完成后恢复阻塞模式，并设置后续收发超时
    if (!socket.setNonBlocking(false) || !socket.setTimeout(timeout_ms)) {
        return std::nullopt;
    }

    return socket;
}

// 设置 socket 收发超时
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

// 通过文件状态标志切换阻塞模式
bool TcpSocket::setNonBlocking(bool enabled) {
    if (fd_ < 0) {
        return false;
    }

    // 保留已有标志，只修改 O_NONBLOCK
    const auto flags = fcntl(fd_, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }

    const auto updated_flags = enabled ? (flags | O_NONBLOCK)
                                       : (flags & ~O_NONBLOCK);
    return fcntl(fd_, F_SETFL, updated_flags) == 0;
}

// 返回借用的文件描述符，关闭操作仍由 TcpSocket 负责
int TcpSocket::fd() const noexcept {
    return fd_;
}

// 发送字符串中的全部数据
bool TcpSocket::sendAll(const std::string& data) {
    // 转给 buffer 版本发送
    return sendAll(data.data(), data.size());
}

// 循环发送直到指定数据全部写入
bool TcpSocket::sendAll(const char* data, size_t len) {
    if (fd_ < 0) {
        return false;
    }

    // 循环发送到指定长度
    size_t sent = 0;

    while (sent < len) {
        const auto n = sendSome(data + sent, len - sent);
        if (n > 0) {
            sent += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n == 0) {
            errno = EPIPE;
        }
        if (n <= 0) {
            return false;
        }
    }

    return true;
}

// 执行一次 send，不循环等待全部数据发送完成
ssize_t TcpSocket::sendSome(const char* data, size_t len) {
    if (fd_ < 0) {
        return -1;
    }
    return send(fd_, data, len, MSG_NOSIGNAL);
}

// 执行一次 recv，不循环等待指定长度
ssize_t TcpSocket::recvSome(char* data, size_t len) {
    if (fd_ < 0) {
        return -1;
    }
    return recv(fd_, data, len, 0);
}

// 循环接收直到填满指定缓冲区
bool TcpSocket::recvAll(char* data, size_t len) {
    if (fd_ < 0) {
        return false;
    }

    // 循环接收到指定长度
    size_t received = 0;

    while (received < len) {
        const auto n = recv(fd_, data + received, len - received, 0);
        if (n > 0) {
            received += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n == 0) {
            errno = ECONNRESET;
        }
        if (n <= 0) {
            return false;
        }
    }

    return true;
}

bool TcpSocket::recvAll(std::string& out, size_t len) {
    // 扩容后直接接收
    out.resize(len);
    return recvAll(out.data(), len);
}

// 按行接收注册中心文本协议
std::optional<std::string> TcpSocket::recvLine(size_t max_size) {
    if (fd_ < 0) {
        return std::nullopt;
    }

    std::string line;

    char ch = '\0';
    while (true) {
        const auto n = recv(fd_, &ch, 1, 0);
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n == 0) {
            errno = ECONNRESET;
            return std::nullopt;
        }
        if (n < 0) {
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
