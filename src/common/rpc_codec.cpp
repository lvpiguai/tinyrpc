#include <tinyrpc/common/rpc_codec.h>

#include <tinyrpc/common/tcp_socket.h>

#include <arpa/inet.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace tinyrpc {

namespace {

constexpr uint32_t kMaxRpcHeaderSize = 64 * 1024;
constexpr uint32_t kMaxRpcBodySize = 16 * 1024 * 1024;

RpcCodecStatus validateBodySizeForSend(std::size_t actual_size, uint32_t declared_size) {
    if (actual_size > kMaxRpcBodySize || declared_size > kMaxRpcBodySize) {
        return RpcCodecStatus::BODY_TOO_LARGE;
    }

    if (actual_size != declared_size) {
        return RpcCodecStatus::BODY_SIZE_MISMATCH;
    }

    return RpcCodecStatus::OK;
}

} // namespace

const char* rpcCodecStatusToString(RpcCodecStatus status) {
    switch (status) {
    case RpcCodecStatus::OK:
        return "rpc codec ok";
    case RpcCodecStatus::SOCKET_ERROR:
        return "rpc socket error";
    case RpcCodecStatus::HEADER_TOO_LARGE:
        return "rpc header too large";
    case RpcCodecStatus::BODY_TOO_LARGE:
        return "rpc body too large";
    case RpcCodecStatus::HEADER_PARSE_FAILED:
        return "rpc header parse failed";
    case RpcCodecStatus::BODY_SIZE_MISMATCH:
        return "rpc body size mismatch";
    case RpcCodecStatus::HEADER_SERIALIZE_FAILED:
        return "rpc header serialize failed";
    }

    return "unknown rpc codec error";
}

RpcCodecStatus RpcCodec::sendRequest(TcpSocket& socket,
                                      const RpcRequestHeader& header,
                                      const std::string& request_body) {
    const auto status = validateBodySizeForSend(request_body.size(), header.request_size());
    if (status != RpcCodecStatus::OK) {
        return status;
    }

    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        return RpcCodecStatus::HEADER_SERIALIZE_FAILED;
    }

    if (header_str.size() > kMaxRpcHeaderSize) {
        return RpcCodecStatus::HEADER_TOO_LARGE;
    }

    const auto header_size = static_cast<uint32_t>(header_str.size());
    const auto net_header_size = htonl(header_size);

    if (!socket.sendAll(reinterpret_cast<const char*>(&net_header_size), sizeof(net_header_size)) ||
        !socket.sendAll(header_str) ||
        !socket.sendAll(request_body)) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    return RpcCodecStatus::OK;
}

RpcCodecStatus RpcCodec::recvRequest(TcpSocket& socket,
                                      RpcRequestHeader& header,
                                      std::string& request_body) {
    uint32_t net_header_size = 0;

    if (!socket.recvAll(reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size))) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    const auto header_size = ntohl(net_header_size);
    if (header_size > kMaxRpcHeaderSize) {
        return RpcCodecStatus::HEADER_TOO_LARGE;
    }

    std::string header_str;
    if (!socket.recvAll(header_str, header_size)) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    if (!header.ParseFromString(header_str)) {
        return RpcCodecStatus::HEADER_PARSE_FAILED;
    }

    const auto request_size = header.request_size();
    if (request_size > kMaxRpcBodySize) {
        return RpcCodecStatus::BODY_TOO_LARGE;
    }

    if (!socket.recvAll(request_body, request_size)) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    return RpcCodecStatus::OK;
}

RpcCodecStatus RpcCodec::sendResponse(TcpSocket& socket,
                                       const RpcResponseHeader& header,
                                       const std::string& response_body) {
    const auto status = validateBodySizeForSend(response_body.size(), header.response_size());
    if (status != RpcCodecStatus::OK) {
        return status;
    }

    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        return RpcCodecStatus::HEADER_SERIALIZE_FAILED;
    }

    if (header_str.size() > kMaxRpcHeaderSize) {
        return RpcCodecStatus::HEADER_TOO_LARGE;
    }

    const auto header_size = static_cast<uint32_t>(header_str.size());
    const auto net_header_size = htonl(header_size);

    if (!socket.sendAll(reinterpret_cast<const char*>(&net_header_size), sizeof(net_header_size)) ||
        !socket.sendAll(header_str) ||
        !socket.sendAll(response_body)) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    return RpcCodecStatus::OK;
}

RpcCodecStatus RpcCodec::recvResponse(TcpSocket& socket,
                                       RpcResponseHeader& header,
                                       std::string& response_body) {
    uint32_t net_header_size = 0;

    if (!socket.recvAll(reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size))) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    const auto header_size = ntohl(net_header_size);
    if (header_size > kMaxRpcHeaderSize) {
        return RpcCodecStatus::HEADER_TOO_LARGE;
    }

    std::string header_str;
    if (!socket.recvAll(header_str, header_size)) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    if (!header.ParseFromString(header_str)) {
        return RpcCodecStatus::HEADER_PARSE_FAILED;
    }

    if (header.response_size() > kMaxRpcBodySize) {
        return RpcCodecStatus::BODY_TOO_LARGE;
    }

    if (!socket.recvAll(response_body, header.response_size())) {
        return RpcCodecStatus::SOCKET_ERROR;
    }

    return RpcCodecStatus::OK;
}

} // namespace tinyrpc
