#include <tinyrpc/common/protocol_codec.h>

#include <google/protobuf/message.h>

namespace tinyrpc {

namespace {

// 校验消息体实际长度和协议头声明长度
RpcStatus validateBodySize(std::size_t actual_size, uint32_t declared_size) {
    // 限制消息体最大长度
    if (actual_size > ProtocolCodec::kMaxBodySize ||
        declared_size > ProtocolCodec::kMaxBodySize) {
        return {RpcErrorCode::BODY_TOO_LARGE, "rpc body too large"};
    }

    // 保证消息体长度与协议头一致
    if (actual_size != declared_size) {
        return {RpcErrorCode::BODY_SIZE_MISMATCH, "rpc body size mismatch"};
    }

    return {};
}

// 将 protobuf 协议头序列化为字节串
RpcStatus encodeHeader(const google::protobuf::Message& header,
                       std::string& encoded_header) {
    // 序列化协议头
    if (!header.SerializeToString(&encoded_header)) {
        return {RpcErrorCode::HEADER_SERIALIZE_FAILED,
                "serialize rpc header failed"};
    }

    // 限制序列化后的协议头长度
    if (encoded_header.size() > ProtocolCodec::kMaxHeaderSize) {
        return {RpcErrorCode::HEADER_TOO_LARGE, "rpc header too large"};
    }

    return {};
}

// 将协议头字节串解析为 protobuf 对象
RpcStatus decodeHeader(const std::string& encoded_header,
                       google::protobuf::Message& header) {
    // 解析前校验协议头长度
    if (encoded_header.size() > ProtocolCodec::kMaxHeaderSize) {
        return {RpcErrorCode::HEADER_TOO_LARGE, "rpc header too large"};
    }

    // 反序列化协议头
    if (!header.ParseFromString(encoded_header)) {
        return {RpcErrorCode::HEADER_PARSE_FAILED, "parse rpc header failed"};
    }

    return {};
}

// 校验协议头声明的消息体长度
RpcStatus validateDeclaredBodySize(uint32_t body_size) {
    if (body_size > ProtocolCodec::kMaxBodySize) {
        return {RpcErrorCode::BODY_TOO_LARGE, "rpc body too large"};
    }

    return {};
}

} // namespace

RpcStatus ProtocolCodec::encodeRequestHeader(const RpcRequestHeader& header,
                                             std::size_t body_size,
                                             std::string& encoded_header) {
    // 校验请求体长度
    const auto status = validateBodySize(body_size, header.request_size());
    if (!status.ok()) {
        return status;
    }

    // 序列化请求头
    return encodeHeader(header, encoded_header);
}

RpcStatus ProtocolCodec::decodeRequestHeader(const std::string& encoded_header,
                                             RpcRequestHeader& header) {
    // 反序列化请求头
    const auto status = decodeHeader(encoded_header, header);
    if (!status.ok()) {
        return status;
    }

    // 校验请求头声明的请求体长度
    return validateDeclaredBodySize(header.request_size());
}

RpcStatus ProtocolCodec::encodeResponseHeader(const RpcResponseHeader& header,
                                              std::size_t body_size,
                                              std::string& encoded_header) {
    // 校验响应体长度
    const auto status = validateBodySize(body_size, header.response_size());
    if (!status.ok()) {
        return status;
    }

    // 序列化响应头
    return encodeHeader(header, encoded_header);
}

RpcStatus ProtocolCodec::decodeResponseHeader(const std::string& encoded_header,
                                              RpcResponseHeader& header) {
    // 反序列化响应头
    const auto status = decodeHeader(encoded_header, header);
    if (!status.ok()) {
        return status;
    }

    // 校验响应头声明的响应体长度
    return validateDeclaredBodySize(header.response_size());
}

} // namespace tinyrpc
