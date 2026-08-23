#include <tinyrpc/client/rpc_channel.h>

#include <tinyrpc/common/protocol_codec.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/rpc_transport.h>
#include <tinyrpc/common/tcp_socket.h>
#include "rpc_header.pb.h"

#include <cstdint>
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
    // 序列化 protobuf 请求对象为请求体字节
    std::string request_body;
    if (!request->SerializeToString(&request_body)) {
        std::string err = "serialize request failed";
        if (controller) {
            controller->SetFailed(err);
        }
        return;
    }

    // 构造 RPC 请求头
    RpcRequestHeader header;
    header.set_service_name(method->service()->full_name());
    header.set_method_name(method->name());
    header.set_request_size(static_cast<uint32_t>(request_body.size()));

    auto target_ip = ip_;
    auto target_port = port_;
    if (use_registry_) {
        RegistryClient registry(registry_ip_, registry_port_);
        if (!registry.discoverService(header.service_name(), target_ip, target_port)) {
            std::string err = "discover service failed";
            if (controller) {
                controller->SetFailed(err);
            }
            return;
        }
    }

    // 连接服务端
    auto socket = TcpSocket::connect(target_ip, target_port);
    if (!socket) {
        std::string err = "connect server failed";
        if (controller) {
            controller->SetFailed(err);
        }
        return;
    }

    RpcTransport transport(*socket);

    // 发送 RPC 请求报文
    auto status = transport.sendRequest(header, request_body);
    if (!status.ok()) {
        if (controller) {
            controller->SetFailed(status.message);
        }
        return;
    }

    // 接收 RPC 响应报文
    RpcResponseHeader response_header;
    std::string response_body;
    status = transport.receiveResponse(response_header, response_body);
    if (!status.ok()) {
        if (controller) {
            controller->SetFailed(status.message);
        }
        return;
    }

    if (response_header.status_code() != RPC_OK) {
        auto err = response_header.status_text();
        if (err.empty()) {
            err = "rpc server error: " + std::to_string(response_header.status_code());
        }
        if (controller) {
            controller->SetFailed(err);
        }
        return;
    }

    // 解析 protobuf 响应对象
    if (!response->ParseFromString(response_body)) {
        std::string err = "parse response failed";
        if (controller) {
            controller->SetFailed(err);
        }
        return;
    }

    // 通知调用方完成
    if (done) {
        done->Run();
    }
}

} // namespace tinyrpc
