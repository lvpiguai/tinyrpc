#include <tinyrpc/common/tcp_frame_transport.h>

#include <tinyrpc/common/tcp_socket.h>

#include <arpa/inet.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace tinyrpc {

namespace {

// 帧长度字段固定为 4 字节
constexpr std::size_t kFrameSizeFieldSize = sizeof(uint32_t);

} // namespace

// 绑定一个已连接的 TCP socket
TcpFrameTransport::TcpFrameTransport(TcpSocket& socket)
    : socket_(socket) {}

// 发送 [frame_size][frame]
RpcStatus TcpFrameTransport::sendFrame(std::string_view frame) {
    // 限制帧最大长度
    if (frame.size() > kMaxFrameSize) {
        return {RpcErrorCode::FRAME_TOO_LARGE, "rpc frame too large"};
    }

    // 将帧长度转换为网络字节序
    const auto frame_size = static_cast<uint32_t>(frame.size());
    const auto network_frame_size = htonl(frame_size);

    // 依次发送帧长度和完整帧
    if (!socket_.sendAll(reinterpret_cast<const char*>(&network_frame_size),
                         kFrameSizeFieldSize) ||
        !socket_.sendAll(frame.data(), frame.size())) {
        return {RpcErrorCode::NETWORK_ERROR, "send rpc frame failed"};
    }

    return {};
}

// 接收 frame_size，再接收完整 frame
RpcStatus TcpFrameTransport::receiveFrame(std::string& frame) {
    // 先读取固定长度的 frame_size
    uint32_t network_frame_size = 0;
    if (!socket_.recvAll(reinterpret_cast<char*>(&network_frame_size),
                         kFrameSizeFieldSize)) {
        return {RpcErrorCode::NETWORK_ERROR, "receive rpc frame size failed"};
    }

    // 转换为主机字节序并校验帧长度
    const auto frame_size = ntohl(network_frame_size);
    if (frame_size > kMaxFrameSize) {
        return {RpcErrorCode::FRAME_TOO_LARGE, "rpc frame too large"};
    }

    // 根据 frame_size 接收完整帧
    if (!socket_.recvAll(frame, frame_size)) {
        return {RpcErrorCode::NETWORK_ERROR, "receive rpc frame failed"};
    }

    return {};
}

} // namespace tinyrpc
