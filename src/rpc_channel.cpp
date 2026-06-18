#include "rpc_channel.h"

#include "registry_client.h"
#include "rpc_codec.h"
#include "rpc_header.pb.h"
#include "tcp_socket.h"

#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <string>

namespace tinyrpc {

RpcChannel::RpcChannel()
    : port_(0) {}

RpcChannel::RpcChannel(const std::string& ip, uint16_t port)
    : ip_(ip), port_(port) {}

void RpcChannel::setRegistry(const std::string& ip, uint16_t port) {
    use_registry_ = true;
    registry_ip_ = ip;
    registry_port_ = port;
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
    // 序列化 protobuf 请求对象为参数字节
    std::string args_str;
    if (!request->SerializeToString(&args_str)) {
        std::string err = "serialize request failed";
        if (controller) {
            controller->SetFailed(err);
        }
        std::cerr << err << std::endl;
        return;
    }

    // 构造 RPC 请求头
    RpcHeader header;
    header.set_service_name(method->service()->full_name());
    header.set_method_name(method->name());
    header.set_args_size(static_cast<uint32_t>(args_str.size()));

    std::string target_ip = ip_;
    uint16_t target_port = port_;
    if (use_registry_) {
        RegistryClient registry(registry_ip_, registry_port_);
        if (!registry.discoverService(header.service_name(), target_ip, target_port)) {
            std::string err = "discover service failed";
            if (controller) {
                controller->SetFailed(err);
            }
            std::cerr << err << ": " << header.service_name() << std::endl;
            return;
        }
    }

    // 连接服务端
    int fd = TcpSocket::connectToServer(target_ip, target_port);
    if (fd < 0) {
        std::string err = "connect server failed";
        if (controller) {
            controller->SetFailed(err);
        }
        std::cerr << err << std::endl;
        return;
    }

    // 发送 RPC 请求消息
    if (!RpcCodec::sendRequest(fd, header, args_str)) {
        std::string err = "send rpc request failed";
        std::cerr << err << std::endl;
        if (controller) {
            controller->SetFailed(err);
        }
        close(fd);
        return;
    }

    // 接收 RPC 响应字节
    std::string response_str;
    if (!RpcCodec::recvResponse(fd, response_str)) {
        std::string err = "recv rpc response failed";
        std::cerr << err << std::endl;
        if (controller) {
            controller->SetFailed(err);
        }
        close(fd);
        return;
    }

    // 解析 protobuf 响应对象
    if (!response->ParseFromString(response_str)) {
        std::string err = "parse response failed";
        if (controller) {
            controller->SetFailed(err);
        }
        std::cerr << err << std::endl;
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
