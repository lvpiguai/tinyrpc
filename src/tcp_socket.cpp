#include "tcp_socket.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

namespace tinyrpc {

int TcpSocket::connectToServer(const std::string& ip, uint16_t port) {
    // 创建 TCP socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    // 填写服务端地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    // 转换 IP 地址
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(fd);
        return -1;
    }

    // 发起连接
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }

    return fd;
}

int TcpSocket::createServerSocket(const std::string& ip, uint16_t port) {
    // 创建监听 socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return -1;
    }

    // 允许端口复用
    int opt = 1;
    if(setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))<0){
        perror("setsockopt");
        close(listen_fd);
        return -1;
    }

    // 绑定监听地址
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(listen_fd);
        return -1;
    }

    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return -1;
    }

    // 开始监听
    if (listen(listen_fd, 16) < 0) {
        perror("listen");
        close(listen_fd);
        return -1;
    }

    return listen_fd;
}

bool TcpSocket::sendAll(int fd, const std::string& data) {
    // 转给 buffer 版本发送
    return sendAll(fd, data.data(), data.size());
}

bool TcpSocket::sendAll(int fd, const char* data, size_t len) {
    // 循环发送到指定长度
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, 0);
        if (n <= 0) {
            return false;
        }
        sent += static_cast<size_t>(n);
    }

    return true;
}

bool TcpSocket::recvAll(int fd, char* data, size_t len) {
    // 循环接收到指定长度
    size_t received = 0;

    while (received < len) {
        ssize_t n = recv(fd, data + received, len - received, 0);
        if (n <= 0) {
            return false;
        }
        received += static_cast<size_t>(n);
    }

    return true;
}

bool TcpSocket::recvAll(int fd, std::string& out, size_t len) {
    // 扩容后直接接收
    out.resize(len);
    return recvAll(fd, out.data(), len);
}

bool TcpSocket::recvLine(int fd, std::string& line) {
    line.clear();

    char ch = '\0';
    while (true) {
        ssize_t n = recv(fd, &ch, 1, 0);
        if (n <= 0) {
            return false;
        }

        if (ch == '\n') {
            return true;
        }

        if (ch != '\r') {
            line.push_back(ch);
        }
    }
}

} // namespace tinyrpc
