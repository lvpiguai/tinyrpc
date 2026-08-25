#include <tinyrpc/server/rpc_server.h>

#include <tinyrpc/common/protocol_codec.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/tcp_listener.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/common/tcp_frame_transport.h>
#include <tinyrpc/common/thread_pool.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace tinyrpc {

namespace {

// 编码并发送 RPC 响应帧
bool sendRpcResponse(TcpFrameTransport& transport, const RpcResponse& response) {
    std::string frame;
    const auto status = protocol_codec::encode(response, frame);
    return status.ok() && transport.sendFrame(frame).ok();
}

// 发送成功响应
bool sendSuccessResponse(TcpFrameTransport& transport,
                         const std::string& response_body) {
    RpcResponse response;
    response.body = response_body;
    return sendRpcResponse(transport, response);
}

// 发送错误响应
bool sendErrorResponse(TcpFrameTransport& transport,
                       const std::string& error_message) {
    RpcResponse response;
    response.success = false;
    response.error_message = error_message;
    return sendRpcResponse(transport, response);
}

} // namespace

// 配置服务端监听地址
RpcServer::RpcServer(Endpoint listen_endpoint)
    : listen_endpoint_(std::move(listen_endpoint)) {}

// 注册本地业务服务
void RpcServer::registerService(google::protobuf::Service& service) {
    // 获取 service 描述
    const auto* service_desc = service.GetDescriptor();

    // 按 service 全名注册
    services_[service_desc->full_name()] = &service;
}

// 配置注册中心地址
void RpcServer::setRegistry(Endpoint registry_endpoint) {
    registry_endpoint_ = std::move(registry_endpoint);
}

// 配置请求处理线程池
void RpcServer::setThreadPool(size_t worker_count, size_t max_queue_size) {
    worker_count_ = worker_count;
    max_queue_size_ = max_queue_size;
}

// 启动 RPC 服务端
void RpcServer::run() {
    // 创建监听 socket
    auto listener = TcpListener::bind(listen_endpoint_.ip,
                                      listen_endpoint_.port);
    if (!listener) {
        std::cerr << "create server socket failed" << std::endl;
        return;
    }

    // 确定工作线程数，0 表示根据硬件自动选择
    auto worker_count = worker_count_;
    if (worker_count == 0) {
        worker_count = std::thread::hardware_concurrency();
        if (worker_count == 0) {
            // 无法获取硬件并发数时使用默认值
            worker_count = 4;
        }
    }

    // 创建 RPC 请求处理线程池
    ThreadPool thread_pool(worker_count, max_queue_size_);

    // 启用注册中心时，发布当前服务端提供的全部服务
    if (registry_endpoint_) {
        RegistryClient registry(*registry_endpoint_);
        for (const auto& item : services_) {
            if (!registry.registerServiceEndpoint(item.first,
                                                  listen_endpoint_)) {
                std::cerr << "register service to registry failed: " << item.first << std::endl;
            }
        }
    }

    // 循环处理客户端连接
    while (true) {
        auto client_socket = listener->accept();
        if (!client_socket) {
            std::cerr << "accept client failed" << std::endl;
            continue;
        }

        // std::function 需要可复制任务，用 shared_ptr 将可移动 Socket 交给工作线程
        auto shared_socket = std::make_shared<TcpSocket>(std::move(*client_socket));

        // 将客户端连接交给线程池处理
        const auto submitted = thread_pool.submit([this, shared_socket]() {
            handleClient(std::move(*shared_socket));
        });
        if (!submitted) {
            std::cerr << "rpc thread pool queue is full, reject connection" << std::endl;
        }
    }
}

// 处理单个客户端连接
void RpcServer::handleClient(TcpSocket client_socket) {
    TcpFrameTransport transport(client_socket);

    // 接收 RPC 请求帧
    std::string frame;
    auto status = transport.receiveFrame(frame);
    if (!status.ok()) {
        return;
    }

    // 解码 RPC 请求帧
    RpcRequest rpc_request;
    status = protocol_codec::decode(frame, rpc_request);
    if (!status.ok()) {
        return;
    }

    // 读取目标服务和方法
    const auto& service_name = rpc_request.service_name;
    const auto& method_name = rpc_request.method_name;

    // 查找目标服务
    const auto service_it = services_.find(service_name);
    if (service_it == services_.end()) {
        const auto err = "service not found: " + service_name;
        sendErrorResponse(transport, err);
        return;
    }

    // 查找目标方法
    auto* service = service_it->second;
    const auto* method =
        service->GetDescriptor()->FindMethodByName(method_name);
    if (method == nullptr) {
        const auto err = "method not found: " + method_name;
        sendErrorResponse(transport, err);
        return;
    }

    // 创建 protobuf 请求和响应对象
    std::unique_ptr<google::protobuf::Message> request(
        service->GetRequestPrototype(method).New());
    std::unique_ptr<google::protobuf::Message> response(
        service->GetResponsePrototype(method).New());

    // 解析 protobuf 请求对象
    if (!request->ParseFromString(rpc_request.body)) {
        std::string err = "parse request body failed";
        sendErrorResponse(transport, err);
        return;
    }

    // 同步调用业务方法
    service->CallMethod(method,
                        nullptr,
                        request.get(),
                        response.get(),
                        nullptr);

    // 序列化 protobuf 响应对象
    std::string response_body;
    if (!response->SerializeToString(&response_body)) {
        sendErrorResponse(transport, "serialize response failed");
        return;
    }

    // 回写 RPC 响应消息
    sendSuccessResponse(transport, response_body);
}

} // namespace tinyrpc
