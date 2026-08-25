#include <tinyrpc/client/rpc_channel.h>

#include <tinyrpc/common/protocol_codec.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/common/tcp_frame_transport.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstdint>
#include <string>
#include <utility>

namespace tinyrpc {

RpcChannel::RpcChannel(Mode mode, Endpoint endpoint)
    : mode_(mode), endpoint_(std::move(endpoint)) {}

// 查找直连或注册中心服务端点
std::optional<Endpoint> RpcChannel::findServiceEndpoint(
    const std::string& service_name) const {
    // 使用直连服务端地址
    if (mode_ == Mode::Direct) {
        return endpoint_;
    }

    // 从注册中心发现服务地址
    RegistryClient registry(endpoint_);
    return registry.discoverServiceEndpoint(service_name);
}

// 执行一次同步 RPC 调用
void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
    // 统一记录错误并通知调用方
    auto fail = [&](const std::string& message) {
        if (controller) {
            controller->SetFailed(message);
        }
        if (done) {
            done->Run();
        }
    };

    // 序列化请求体
    std::string request_body;
    if (!request->SerializeToString(&request_body)) {
        fail("serialize request failed");
        return;
    }

    // 构造 RPC 请求对象
    RpcRequest rpc_request{
        method->service()->full_name(),
        method->name(),
        std::move(request_body)
    };

    // 编码 RPC 请求帧
    std::string frame;
    auto status = protocol_codec::encode(rpc_request, frame);
    if (!status.ok()) {
        fail(status.message);
        return;
    }

    // 获取目标服务地址
    const auto target_endpoint = findServiceEndpoint(rpc_request.service_name);
    if (!target_endpoint) {
        fail("discover service failed");
        return;
    }

    // 连接服务端
    auto socket = TcpSocket::connect(target_endpoint->ip,
                                     target_endpoint->port);
    if (!socket) {
        fail("connect server failed");
        return;
    }

    TcpFrameTransport transport(*socket);

    // 发送 RPC 请求帧
    status = transport.sendFrame(frame);
    if (!status.ok()) {
        fail(status.message);
        return;
    }

    // 接收 RPC 响应帧
    status = transport.receiveFrame(frame);
    if (!status.ok()) {
        fail(status.message);
        return;
    }

    // 解码 RPC 响应帧
    RpcResponse rpc_response;
    status = protocol_codec::decode(frame, rpc_response);
    if (!status.ok()) {
        fail(status.message);
        return;
    }

    // 检查服务端处理结果
    if (!rpc_response.success) {
        auto err = rpc_response.error_message;
        if (err.empty()) {
            err = "rpc server error";
        }
        fail(err);
        return;
    }

    // 解析 protobuf 响应对象
    if (!response->ParseFromString(rpc_response.body)) {
        fail("parse response failed");
        return;
    }

    // 通知调用方完成
    if (done) {
        done->Run();
    }
}

} // namespace tinyrpc
