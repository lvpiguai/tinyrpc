#include <tinyrpc/common/rpc_status.h>
#include <tinyrpc/common/tcp_frame_transport.h>
#include <tinyrpc/common/tcp_socket.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <sys/socket.h>

int main() {
    int fds[2] = {-1, -1};
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) < 0) {
        std::cerr << "create socket pair failed" << std::endl;
        return 1;
    }

    tinyrpc::TcpSocket receiver(fds[0]);
    tinyrpc::TcpSocket peer(fds[1]);
    if (!receiver.setTimeout(80)) {
        std::cerr << "set socket timeout failed: " << std::strerror(errno)
                  << std::endl;
        return 1;
    }

    // 对端不发送数据，receiveFrame 应在超时后返回 TIMEOUT
    tinyrpc::TcpFrameTransport transport(receiver);
    std::string frame;
    const auto begin = std::chrono::steady_clock::now();
    const auto status = transport.receiveFrame(frame);
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - begin);

    if (status.code != tinyrpc::RpcErrorCode::TIMEOUT) {
        std::cerr << "expected timeout status" << std::endl;
        return 1;
    }
    if (elapsed < std::chrono::milliseconds(40)) {
        std::cerr << "socket returned before configured timeout" << std::endl;
        return 1;
    }

    // 非法超时参数应在发起网络操作前直接失败
    errno = 0;
    const auto invalid = tinyrpc::TcpSocket::connect("127.0.0.1", 1, 0);
    if (invalid || errno != EINVAL) {
        std::cerr << "invalid connect timeout was not rejected" << std::endl;
        return 1;
    }

    return 0;
}
