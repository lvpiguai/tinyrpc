#pragma once

#include "rpc_header.pb.h"

#include <string>

namespace tinyrpc {

// 负责按 TinyRPC 协议收发 RPC 消息字节
class RpcCodec {
public:
    static bool sendRequest(int fd,
                            const RpcHeader& header,
                            const std::string& args);

    static bool recvRequest(int fd,
                            RpcHeader& header,
                            std::string& args);

    static bool sendResponse(int fd,
                             const std::string& response);

    static bool recvResponse(int fd,
                             std::string& response);
};

} // namespace tinyrpc
