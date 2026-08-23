#include <tinyrpc/server/rpc_provider.h>

#include <tinyrpc/common/protocol_codec.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/rpc_transport.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/server/thread_pool.h>
#include "rpc_header.pb.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace tinyrpc {

namespace {

bool sendSuccessResponse(RpcTransport& transport, const std::string& response_body) {
    RpcResponseHeader header;
    header.set_status_code(RPC_OK);
    header.set_status_text("");
    header.set_response_size(static_cast<uint32_t>(response_body.size()));

    return transport.sendResponse(header, response_body).ok();
}

bool sendErrorResponse(RpcTransport& transport,
                       int32_t status_code,
                       const std::string& status_text) {
    RpcResponseHeader header;
    header.set_status_code(status_code);
    header.set_status_text(status_text);
    header.set_response_size(0);

    return transport.sendResponse(header, "").ok();
}

} // namespace

// 业务方法完成后回写响应
class SendResponseClosure : public google::protobuf::Closure {
public:
    SendResponseClosure(TcpSocket client_socket, google::protobuf::Message* response)
        : client_socket_(std::move(client_socket)), response_(response) {}

    void Run() override {
        RpcTransport transport(client_socket_);

        // 序列化 protobuf 响应对象
        std::string response_body;

        if (!response_->SerializeToString(&response_body)) {
            sendErrorResponse(transport,
                              RPC_SERIALIZE_RESPONSE_FAILED,
                              "serialize response failed");
            delete response_;
            delete this;
            return;
        }

        // 回写 RPC 响应消息
        sendSuccessResponse(transport, response_body);

        delete response_;
        delete this;
    }

private:
    TcpSocket client_socket_;
    google::protobuf::Message* response_;
};

void RpcProvider::registerService(google::protobuf::Service& service) {
    // 获取 service 描述
    const auto* service_desc = service.GetDescriptor();

    // 按 service 全名注册
    services_[service_desc->full_name()] = &service;
}

void RpcProvider::setRegistry(const std::string& ip, uint16_t port) {
    use_registry_ = true;
    registry_ip_ = ip;
    registry_port_ = port;
}

void RpcProvider::setThreadPool(size_t worker_count, size_t max_queue_size) {
    worker_count_ = worker_count;
    max_queue_size_ = max_queue_size;
}

void RpcProvider::run(const std::string& ip, uint16_t port) {
    // 创建监听 socket
    auto listener = TcpListener::bind(ip, port);
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

    // 启用注册中心时，发布当前 Provider 提供的全部服务
    if (use_registry_) {
        RegistryClient registry(registry_ip_, registry_port_);
        for (const auto& item : services_) {
            if (!registry.registerService(item.first, ip, port)) {
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

void RpcProvider::handleClient(TcpSocket client_socket) {
    RpcTransport transport(client_socket);

    // 接收 RPC 请求报文
    RpcRequestHeader header;
    std::string request_body;

    const auto status = transport.receiveRequest(header, request_body);
    if (!status.ok()) {
        return;
    }

    const auto service_name = header.service_name();
    const auto method_name = header.method_name();

    // 根据请求头查找服务方法
    const auto service_it = services_.find(service_name);
    if (service_it == services_.end()) {
        const auto err = "service not found: " + service_name;
        sendErrorResponse(transport, RPC_SERVICE_NOT_FOUND, err);
        return;
    }

    auto* service = service_it->second;
    const auto* method =
        service->GetDescriptor()->FindMethodByName(method_name);
    if (method == nullptr) {
        const auto err = "method not found: " + method_name;
        sendErrorResponse(transport, RPC_METHOD_NOT_FOUND, err);
        return;
    }

    // 创建 protobuf 请求和响应对象
    auto* request = service->GetRequestPrototype(method).New();
    auto* response = service->GetResponsePrototype(method).New();

    // 解析 protobuf 请求对象
    if (!request->ParseFromString(request_body)) {
        std::string err = "parse request body failed";
        sendErrorResponse(transport, RPC_PARSE_REQUEST_FAILED, err);
        delete request;
        delete response;
        return;
    }

    // 调用业务方法
    auto* done = new SendResponseClosure(std::move(client_socket), response);

    service->CallMethod(method, nullptr, request, response, done);

    // 释放请求对象
    delete request;
}

} // namespace tinyrpc
