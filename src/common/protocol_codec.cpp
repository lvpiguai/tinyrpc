#include <tinyrpc/common/protocol_codec.h>

#include "rpc_header.pb.h"

#include <arpa/inet.h>
#include <google/protobuf/message.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace tinyrpc {
namespace protocol_codec {

namespace {

// 包头长度字段固定为 4 字节
constexpr std::size_t kHeaderSizeFieldSize = sizeof(uint32_t);

// 将 protobuf 包头序列化为字节串
RpcStatus serializeHeader(const google::protobuf::Message& header,
                          std::string& encoded_header) {
    // 序列化 protobuf 包头
    if (!header.SerializeToString(&encoded_header)) {
        return {RpcErrorCode::HEADER_SERIALIZE_FAILED,
                "serialize rpc header failed"};
    }

    // 限制序列化后的包头长度
    if (encoded_header.size() > kMaxHeaderSize) {
        return {RpcErrorCode::HEADER_TOO_LARGE, "rpc header too large"};
    }

    return {};
}

// 将包头字节串解析为 protobuf 对象
RpcStatus deserializeHeader(std::string_view encoded_header,
                            google::protobuf::Message& header) {
    // 解析前校验包头长度
    if (encoded_header.size() > kMaxHeaderSize) {
        return {RpcErrorCode::HEADER_TOO_LARGE, "rpc header too large"};
    }

    // 反序列化 protobuf 包头
    if (!header.ParseFromArray(encoded_header.data(),
                               static_cast<int>(encoded_header.size()))) {
        return {RpcErrorCode::HEADER_PARSE_FAILED, "parse rpc header failed"};
    }

    return {};
}

// 组装完整 RPC 帧：[header_size][header][body]
RpcStatus buildFrame(const google::protobuf::Message& header,
                     std::string_view body,
                     std::string& frame) {
    // 限制消息体最大长度
    if (body.size() > kMaxBodySize) {
        return {RpcErrorCode::BODY_TOO_LARGE, "rpc body too large"};
    }

    // 序列化 protobuf 包头
    std::string encoded_header;
    const auto status = serializeHeader(header, encoded_header);
    if (!status.ok()) {
        return status;
    }

    // 将包头长度转换为网络字节序
    const auto header_size = static_cast<uint32_t>(encoded_header.size());
    const auto network_header_size = htonl(header_size);

    // 依次写入包头长度、包头和消息体
    frame.clear();
    frame.reserve(kHeaderSizeFieldSize + encoded_header.size() + body.size());
    frame.append(reinterpret_cast<const char*>(&network_header_size),
                 kHeaderSizeFieldSize);
    frame.append(encoded_header);
    frame.append(body.data(), body.size());
    return {};
}

// 将完整 RPC 帧切分为包头和消息体
RpcStatus splitFrame(std::string_view frame,
                     std::string_view& header,
                     std::string_view& body) {
    // frame 必须包含完整的包头长度字段
    if (frame.size() < kHeaderSizeFieldSize) {
        return {RpcErrorCode::HEADER_PARSE_FAILED,
                "incomplete rpc header size"};
    }

    // 读取网络字节序的包头长度
    uint32_t network_header_size = 0;
    std::memcpy(&network_header_size, frame.data(), kHeaderSizeFieldSize);
    const auto header_size = ntohl(network_header_size);

    // 限制包头最大长度
    if (header_size > kMaxHeaderSize) {
        return {RpcErrorCode::HEADER_TOO_LARGE, "rpc header too large"};
    }

    // frame 必须包含完整包头
    if (frame.size() < kHeaderSizeFieldSize + header_size) {
        return {RpcErrorCode::HEADER_PARSE_FAILED, "incomplete rpc header"};
    }

    // 切分包头和消息体字节
    header = frame.substr(kHeaderSizeFieldSize, header_size);
    body = frame.substr(kHeaderSizeFieldSize + header_size);

    // 限制消息体最大长度
    if (body.size() > kMaxBodySize) {
        return {RpcErrorCode::BODY_TOO_LARGE, "rpc body too large"};
    }

    return {};
}

} // namespace

// 编码 RPC 请求帧
RpcStatus encode(const RpcRequest& request, std::string& frame) {
    // 根据逻辑请求组装 protobuf 请求头
    RpcRequestHeader header;
    header.set_service_name(request.service_name);
    header.set_method_name(request.method_name);
    return buildFrame(header, request.body, frame);
}

// 解码 RPC 请求帧
RpcStatus decode(std::string_view frame, RpcRequest& request) {
    // 切分包头和请求体
    std::string_view header;
    std::string_view body;
    auto status = splitFrame(frame, header, body);
    if (!status.ok()) {
        return status;
    }

    // 解析 protobuf 请求头
    RpcRequestHeader request_header;
    status = deserializeHeader(header, request_header);
    if (!status.ok()) {
        return status;
    }

    // 填充逻辑请求
    request.service_name = request_header.service_name();
    request.method_name = request_header.method_name();
    request.body.assign(body.data(), body.size());
    return {};
}

// 编码 RPC 响应帧
RpcStatus encode(const RpcResponse& response, std::string& frame) {
    // 根据逻辑响应组装 protobuf 响应头
    RpcResponseHeader header;
    header.set_success(response.success);
    header.set_error_message(response.error_message);
    return buildFrame(header, response.body, frame);
}

// 解码 RPC 响应帧
RpcStatus decode(std::string_view frame, RpcResponse& response) {
    // 切分包头和响应体
    std::string_view header;
    std::string_view body;
    auto status = splitFrame(frame, header, body);
    if (!status.ok()) {
        return status;
    }

    // 解析 protobuf 响应头
    RpcResponseHeader response_header;
    status = deserializeHeader(header, response_header);
    if (!status.ok()) {
        return status;
    }

    // 填充逻辑响应
    response.success = response_header.success();
    response.error_message = response_header.error_message();
    response.body.assign(body.data(), body.size());
    return {};
}

} // namespace protocol_codec
} // namespace tinyrpc
