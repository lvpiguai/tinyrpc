#pragma once

#include <string>

namespace tinyrpc {

// RPC 请求在编码前、解码后的逻辑数据
struct RpcRequest {
    std::string service_name;
    std::string method_name;
    std::string body;
};

// RPC 响应在编码前、解码后的逻辑数据
struct RpcResponse {
    bool success = true;
    std::string error_message;
    std::string body;
};

} // namespace tinyrpc
