#include "rpc_codec.h"

#include "tcp_socket.h"

#include <arpa/inet.h>

#include <cstdint>
#include <string>

namespace tinyrpc {

bool RpcCodec::sendRequest(int fd,
                           const RpcRequestHeader& header,
                           const std::string& request_body) {
    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        return false;
    }

    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t net_header_size = htonl(header_size);

    return TcpSocket::sendAll(fd, reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size)) &&
           TcpSocket::sendAll(fd, header_str) &&
           TcpSocket::sendAll(fd, request_body);
}

bool RpcCodec::recvRequest(int fd,
                           RpcRequestHeader& header,
                           std::string& request_body) {
    uint32_t net_header_size = 0;

    if (!TcpSocket::recvAll(fd, reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size))) {
        return false;
    }

    uint32_t header_size = ntohl(net_header_size);

    std::string header_str;
    if (!TcpSocket::recvAll(fd, header_str, header_size)) {
        return false;
    }

    if (!header.ParseFromString(header_str)) {
        return false;
    }

    uint32_t request_size = header.request_size();

    if (!TcpSocket::recvAll(fd, request_body, request_size)) {
        return false;
    }

    return true;
}

bool RpcCodec::sendResponse(int fd,
                            const RpcResponseHeader& header,
                            const std::string& response_body) {
    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        return false;
    }

    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t net_header_size = htonl(header_size);

    return TcpSocket::sendAll(fd, reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size)) &&
           TcpSocket::sendAll(fd, header_str) &&
           TcpSocket::sendAll(fd, response_body);
}

bool RpcCodec::recvResponse(int fd,
                            RpcResponseHeader& header,
                            std::string& response_body) {
    uint32_t net_header_size = 0;

    if (!TcpSocket::recvAll(fd, reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size))) {
        return false;
    }

    uint32_t header_size = ntohl(net_header_size);

    std::string header_str;
    if (!TcpSocket::recvAll(fd, header_str, header_size)) {
        return false;
    }

    if (!header.ParseFromString(header_str)) {
        return false;
    }

    return TcpSocket::recvAll(fd, response_body, header.response_size());
}

} // namespace tinyrpc
