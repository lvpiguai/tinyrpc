#include <tinyrpc/common/rpc_transport.h>

#include <tinyrpc/common/protocol_codec.h>
#include <tinyrpc/common/tcp_socket.h>

#include <arpa/inet.h>

#include <cstdint>

namespace tinyrpc {

RpcTransport::RpcTransport(TcpSocket& socket)
    : socket_(socket) {}

RpcStatus RpcTransport::sendRequest(const RpcRequestHeader& header,
                                    const std::string& request_body) {
    // 编码并校验 RPC 请求头
    std::string encoded_header;
    const auto status =
        ProtocolCodec::encodeRequestHeader(header, request_body.size(), encoded_header);
    if (!status.ok()) {
        return status;
    }

    // 发送完整 RPC 请求报文
    return sendMessage(encoded_header, request_body);
}

RpcStatus RpcTransport::receiveRequest(RpcRequestHeader& header,
                                       std::string& request_body) {
    // 接收 RPC 请求头字节
    std::string encoded_header;
    const auto status = receiveHeader(encoded_header);
    if (!status.ok()) {
        return status;
    }

    // 解码并校验 RPC 请求头
    const auto codec_status = ProtocolCodec::decodeRequestHeader(encoded_header, header);
    if (!codec_status.ok()) {
        return codec_status;
    }

    // 根据请求头中的长度接收请求体
    if (!socket_.recvAll(request_body, header.request_size())) {
        return {RpcErrorCode::NETWORK_ERROR, "receive rpc request body failed"};
    }

    return {};
}

RpcStatus RpcTransport::sendResponse(const RpcResponseHeader& header,
                                     const std::string& response_body) {
    // 编码并校验 RPC 响应头
    std::string encoded_header;
    const auto status =
        ProtocolCodec::encodeResponseHeader(header, response_body.size(), encoded_header);
    if (!status.ok()) {
        return status;
    }

    // 发送完整 RPC 响应报文
    return sendMessage(encoded_header, response_body);
}

RpcStatus RpcTransport::receiveResponse(RpcResponseHeader& header,
                                        std::string& response_body) {
    // 接收 RPC 响应头字节
    std::string encoded_header;
    const auto status = receiveHeader(encoded_header);
    if (!status.ok()) {
        return status;
    }

    // 解码并校验 RPC 响应头
    const auto codec_status = ProtocolCodec::decodeResponseHeader(encoded_header, header);
    if (!codec_status.ok()) {
        return codec_status;
    }

    // 根据响应头中的长度接收响应体
    if (!socket_.recvAll(response_body, header.response_size())) {
        return {RpcErrorCode::NETWORK_ERROR, "receive rpc response body failed"};
    }

    return {};
}

RpcStatus RpcTransport::sendMessage(const std::string& encoded_header,
                                    const std::string& body) {
    // 将报文头长度转换为网络字节序
    const auto header_size = static_cast<uint32_t>(encoded_header.size());
    const auto network_header_size = htonl(header_size);

    // 依次发送报文头长度、报文头和消息体
    if (!socket_.sendAll(reinterpret_cast<const char*>(&network_header_size),
                         sizeof(network_header_size)) ||
        !socket_.sendAll(encoded_header) ||
        !socket_.sendAll(body)) {
        return {RpcErrorCode::NETWORK_ERROR, "send rpc message failed"};
    }

    return {};
}

RpcStatus RpcTransport::receiveHeader(std::string& encoded_header) {
    // 接收网络字节序的报文头长度
    uint32_t network_header_size = 0;
    if (!socket_.recvAll(reinterpret_cast<char*>(&network_header_size),
                         sizeof(network_header_size))) {
        return {RpcErrorCode::NETWORK_ERROR, "receive rpc header size failed"};
    }

    // 转换并校验报文头长度
    const auto header_size = ntohl(network_header_size);
    if (header_size > ProtocolCodec::kMaxHeaderSize) {
        return {RpcErrorCode::HEADER_TOO_LARGE, "rpc header too large"};
    }

    // 接收指定长度的报文头
    if (!socket_.recvAll(encoded_header, header_size)) {
        return {RpcErrorCode::NETWORK_ERROR, "receive rpc header failed"};
    }

    return {};
}

} // namespace tinyrpc
