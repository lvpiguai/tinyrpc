#include "rpc_codec.h"

#include "tcp_socket.h"

#include <arpa/inet.h>

#include <cstdint>
#include <string>

namespace tinyrpc {

bool RpcCodec::sendRequest(int fd,
                           const RpcHeader& header,
                           const std::string& args) {
    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        return false;
    }

    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t net_header_size = htonl(header_size);

    return TcpSocket::sendAll(fd, reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size)) &&
           TcpSocket::sendAll(fd, header_str) &&
           TcpSocket::sendAll(fd, args);
}

bool RpcCodec::recvRequest(int fd,
                           RpcHeader& header,
                           std::string& args) {
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

    uint32_t args_size = header.args_size();

    if (!TcpSocket::recvAll(fd, args, args_size)) {
        return false;
    }

    return true;
}

bool RpcCodec::sendResponse(int fd,
                            const std::string& response) {
    uint32_t response_size = static_cast<uint32_t>(response.size());
    uint32_t net_response_size = htonl(response_size);

    return TcpSocket::sendAll(fd, reinterpret_cast<char*>(&net_response_size), sizeof(net_response_size)) &&
           TcpSocket::sendAll(fd, response);
}

bool RpcCodec::recvResponse(int fd,
                            std::string& response) {
    uint32_t net_response_size = 0;

    if (!TcpSocket::recvAll(fd, reinterpret_cast<char*>(&net_response_size), sizeof(net_response_size))) {
        return false;
    }

    uint32_t response_size = ntohl(net_response_size);

    return TcpSocket::recvAll(fd, response, response_size);
}

} // namespace tinyrpc
