#include "rpc_channel.h"

#include "rpc_header.pb.h"
#include "tcp_socket.h"

#include <arpa/inet.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <string>

namespace tinyrpc {

RpcChannel::RpcChannel(const std::string& ip, uint16_t port)
    : ip_(ip), port_(port) {}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
    // 序列化请求参数
    std::string args_str;
    if (!request->SerializeToString(&args_str)) {
        std::cerr << "serialize request failed" << std::endl;
        return;
    }

    // 组装 RPC 头
    RpcHeader header;
    header.set_service_name(method->service()->full_name());
    header.set_method_name(method->name());
    header.set_args_size(static_cast<uint32_t>(args_str.size()));

    // 序列化头部
    std::string header_str;
    if (!header.SerializeToString(&header_str)) {
        std::cerr << "serialize rpc header failed" << std::endl;
        return;
    }

    // 连接服务端
    int fd = TcpSocket::connectToServer(ip_, port_);
    if (fd < 0) {
        std::cerr << "connect server failed" << std::endl;
        return;
    }

    // 发送头长、头部和参数
    uint32_t header_size = static_cast<uint32_t>(header_str.size());
    uint32_t net_header_size = htonl(header_size);

    if (!TcpSocket::sendAll(fd, reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size)) ||
        !TcpSocket::sendAll(fd, header_str) ||
        !TcpSocket::sendAll(fd, args_str)) {
        std::cerr << "send rpc request failed" << std::endl;
        close(fd);
        return;
    }

    // 读取响应长度
    uint32_t net_response_size = 0;
    if (!TcpSocket::recvAll(fd, reinterpret_cast<char*>(&net_response_size), sizeof(net_response_size))) {
        std::cerr << "recv response size failed" << std::endl;
        close(fd);
        return;
    }

    uint32_t response_size = ntohl(net_response_size);

    // 读取并解析响应体
    std::string response_str;
    if (!TcpSocket::recvAll(fd, response_str, response_size)) {
        std::cerr << "recv response body failed" << std::endl;
        close(fd);
        return;
    }

    if (!response->ParseFromString(response_str)) {
        std::cerr << "parse response failed" << std::endl;
        close(fd);
        return;
    }

    close(fd);

    // 通知调用方完成
    if (done) {
        done->Run();
    }
}

} // namespace tinyrpc
