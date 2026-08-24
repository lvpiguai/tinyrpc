#pragma once

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

// RPC 请求在编码前、解码后的逻辑数据
struct RpcRequest {
    std::string service_name;
    std::string method_name;
    std::string body;
};

// RPC 响应在编码前、解码后的逻辑数据
struct RpcResponse {
    int32_t status_code = RPC_OK;
    std::string status_text;
    std::string body;
};

} // namespace tinyrpc
