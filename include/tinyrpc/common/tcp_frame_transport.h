#pragma once

#include <tinyrpc/common/rpc_status.h>

#include <cstddef>
#include <string>
#include <string_view>

namespace tinyrpc {

class TcpSocket;

// 负责通过 TCP 连接收发带长度前缀的完整帧
class TcpFrameTransport {
public:
    static constexpr std::size_t kMaxFrameSize = 17 * 1024 * 1024;

    // 绑定一个已连接的 TCP socket
    explicit TcpFrameTransport(TcpSocket& socket);

    // 发送 [frame_size][frame]
    RpcStatus sendFrame(std::string_view frame);

    // 接收 frame_size，再接收完整 frame
    RpcStatus receiveFrame(std::string& frame);

private:
    TcpSocket& socket_;
};

} // namespace tinyrpc
