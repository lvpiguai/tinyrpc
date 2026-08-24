#pragma once

#include <string>

namespace tinyrpc {

enum class RpcErrorCode {
    OK = 0,
    NETWORK_ERROR,
    FRAME_TOO_LARGE,
    HEADER_TOO_LARGE,
    BODY_TOO_LARGE,
    HEADER_PARSE_FAILED,
    HEADER_SERIALIZE_FAILED
};

// 保存一次 RPC 操作的错误码和错误信息
struct RpcStatus {
    RpcErrorCode code = RpcErrorCode::OK;
    std::string message;

    // 判断操作是否成功
    bool ok() const {
        return code == RpcErrorCode::OK;
    }
};

} // namespace tinyrpc
