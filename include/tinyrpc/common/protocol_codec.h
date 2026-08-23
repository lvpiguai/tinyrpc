#pragma once

#include <tinyrpc/common/rpc_status.h>

#include "rpc_header.pb.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace tinyrpc {

enum RpcResponseStatusCode {
    RPC_OK = 0,
    RPC_INVALID_REQUEST = 1,
    RPC_SERVICE_NOT_FOUND = 2,
    RPC_METHOD_NOT_FOUND = 3,
    RPC_PARSE_REQUEST_FAILED = 4,
    RPC_SERIALIZE_RESPONSE_FAILED = 5,
    RPC_INTERNAL_ERROR = 6
};

// 负责 RPC 协议头的序列化、解析和长度校验
class ProtocolCodec {
public:
    static constexpr std::size_t kMaxHeaderSize = 64 * 1024;
    static constexpr std::size_t kMaxBodySize = 16 * 1024 * 1024;

    // 编码请求头
    static RpcStatus encodeRequestHeader(const RpcRequestHeader& header,
                                         std::size_t body_size,
                                         std::string& encoded_header);

    // 解码请求头
    static RpcStatus decodeRequestHeader(const std::string& encoded_header,
                                         RpcRequestHeader& header);

    // 编码响应头
    static RpcStatus encodeResponseHeader(const RpcResponseHeader& header,
                                          std::size_t body_size,
                                          std::string& encoded_header);

    // 解码响应头
    static RpcStatus decodeResponseHeader(const std::string& encoded_header,
                                          RpcResponseHeader& header);
};

} // namespace tinyrpc
