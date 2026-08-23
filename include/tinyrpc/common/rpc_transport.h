#pragma once

#include <tinyrpc/common/rpc_status.h>

#include "rpc_header.pb.h"

#include <string>

namespace tinyrpc {

class TcpSocket;

// 负责通过 TCP 连接收发完整 RPC 报文
class RpcTransport {
public:
    // 绑定一个已连接的 TCP socket
    explicit RpcTransport(TcpSocket& socket);

    // 发送 RPC 请求报文
    RpcStatus sendRequest(const RpcRequestHeader& header,
                          const std::string& request_body);

    // 接收 RPC 请求报文
    RpcStatus receiveRequest(RpcRequestHeader& header,
                             std::string& request_body);

    // 发送 RPC 响应报文
    RpcStatus sendResponse(const RpcResponseHeader& header,
                           const std::string& response_body);

    // 接收 RPC 响应报文
    RpcStatus receiveResponse(RpcResponseHeader& header,
                              std::string& response_body);

private:
    // 发送报文头长度、报文头和消息体
    RpcStatus sendMessage(const std::string& encoded_header,
                          const std::string& body);

    // 接收报文头长度和报文头
    RpcStatus receiveHeader(std::string& encoded_header);

private:
    TcpSocket& socket_;
};

} // namespace tinyrpc
