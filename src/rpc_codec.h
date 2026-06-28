#pragma once

#include "rpc_header.pb.h"

#include <cstdint>
#include <string>

namespace tinyrpc {

enum RpcErrorCode {
    RPC_OK = 0,
    RPC_INVALID_REQUEST = 1,
    RPC_SERVICE_NOT_FOUND = 2,
    RPC_METHOD_NOT_FOUND = 3,
    RPC_PARSE_REQUEST_FAILED = 4,
    RPC_SERIALIZE_RESPONSE_FAILED = 5,
    RPC_INTERNAL_ERROR = 6
};

// 负责按 TinyRPC 协议收发 RPC 消息字节
class RpcCodec {
public:
    static bool sendRequest(int fd,
                            const RpcRequestHeader& header,
                            const std::string& request_body);

    static bool recvRequest(int fd,
                            RpcRequestHeader& header,
                            std::string& request_body);

    static bool sendResponse(int fd,
                             const RpcResponseHeader& header,
                             const std::string& response_body);

    static bool recvResponse(int fd,
                             RpcResponseHeader& header,
                             std::string& response_body);
};

} // namespace tinyrpc
