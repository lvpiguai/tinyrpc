#pragma once

#include <tinyrpc/common/rpc_message.h>
#include <tinyrpc/common/rpc_status.h>

#include <cstddef>
#include <string>
#include <string_view>

namespace tinyrpc {

namespace protocol_codec {

// 负责完整 RPC 报文的编码、解码和长度校验
inline constexpr std::size_t kMaxHeaderSize = 64 * 1024;
inline constexpr std::size_t kMaxBodySize = 16 * 1024 * 1024;

// 编码完整 RPC 帧：[header_size][header][body]
RpcStatus encode(const RpcRequest& request, std::string& frame);
RpcStatus encode(const RpcResponse& response, std::string& frame);

// 解码完整 RPC 帧
RpcStatus decode(std::string_view frame, RpcRequest& request);
RpcStatus decode(std::string_view frame, RpcResponse& response);

} // namespace protocol_codec

} // namespace tinyrpc
