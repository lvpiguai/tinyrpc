#include "rpc_provider.h"

#include "registry_client.h"
#include "rpc_codec.h"
#include "rpc_header.pb.h"
#include "tcp_socket.h"
#include "thread_pool.h"

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

namespace tinyrpc {

namespace {

bool sendSuccessResponse(int client_fd, const std::string& response_body) {
    RpcResponseHeader header;
    header.set_error_code(RPC_OK);
    header.set_error_text("");
    header.set_response_size(static_cast<uint32_t>(response_body.size()));

    return RpcCodec::sendResponse(client_fd, header, response_body);
}

bool sendErrorResponse(int client_fd, int32_t error_code, const std::string& error_text) {
    RpcResponseHeader header;
    header.set_error_code(error_code);
    header.set_error_text(error_text);
    header.set_response_size(0);

    return RpcCodec::sendResponse(client_fd, header, "");
}

} // namespace

// 业务方法完成后回写响应
class SendResponseClosure : public google::protobuf::Closure {
public:
    SendResponseClosure(int client_fd, google::protobuf::Message* response)
        : client_fd_(client_fd), response_(response) {}

    void Run() override {
        // 序列化 protobuf 响应对象
        std::string response_body;

        if (!response_->SerializeToString(&response_body)) {
            std::cerr << "serialize response failed" << std::endl;
            sendErrorResponse(client_fd_,
                              RPC_SERIALIZE_RESPONSE_FAILED,
                              "serialize response failed");
            close(client_fd_);
            delete response_;
            delete this;
            return;
        }

        // 回写 RPC 响应消息
        sendSuccessResponse(client_fd_, response_body);

        // 释放资源
        close(client_fd_);

        delete response_;
        delete this;
    }

private:
    int client_fd_;
    google::protobuf::Message* response_;
};

void RpcProvider::registerService(google::protobuf::Service* service) {
    // 获取 service 描述
    const google::protobuf::ServiceDescriptor* service_desc = service->GetDescriptor();

    ServiceInfo service_info;
    service_info.service = service;

    // 建立方法索引
    for (int i = 0; i < service_desc->method_count(); ++i) {
        const google::protobuf::MethodDescriptor* method = service_desc->method(i);
        service_info.methods[method->name()] = method;
    }

    // 按 service 全名注册
    services_[service_desc->full_name()] = service_info;

    std::cout << "register service: " << service_desc->full_name() << std::endl;
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
    int listen_fd = TcpSocket::createServerSocket(ip, port);
    if (listen_fd < 0) {
        std::cerr << "create server socket failed" << std::endl;
        return;
    }

    std::cout << "tinyrpc server start at " << ip << ":" << port << std::endl;

    // 创建 RPC 请求处理线程池
    size_t worker_count = worker_count_;
    if (worker_count == 0) {
        worker_count = std::thread::hardware_concurrency();
        if (worker_count == 0) {
            worker_count = 4;
        }
    }

    ThreadPool thread_pool(worker_count, max_queue_size_);
    std::cout << "rpc thread pool start, workers: " << worker_count
              << ", max queue size: " << max_queue_size_ << std::endl;

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
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        // 将客户端连接交给线程池处理
        bool submitted = thread_pool.submit([this, client_fd]() {
            handleClient(client_fd);
        });
        if (!submitted) {
            std::cerr << "rpc thread pool queue is full, reject connection" << std::endl;
            close(client_fd);
        }
    }
}

void RpcProvider::handleClient(int client_fd) {
    // 接收 RPC 请求头和请求体字节
    RpcRequestHeader header;
    std::string request_body;

    if (!RpcCodec::recvRequest(client_fd, header, request_body)) {
        std::cerr << "recv rpc request failed" << std::endl;
        close(client_fd);
        return;
    }

    std::string service_name = header.service_name();
    std::string method_name = header.method_name();

    // 根据请求头查找服务方法
    auto service_it = services_.find(service_name);
    if (service_it == services_.end()) {
        std::string err = "service not found: " + service_name;
        std::cerr << err << std::endl;
        sendErrorResponse(client_fd, RPC_SERVICE_NOT_FOUND, err);
        close(client_fd);
        return;
    }

    auto method_it = service_it->second.methods.find(method_name);
    if (method_it == service_it->second.methods.end()) {
        std::string err = "method not found: " + method_name;
        std::cerr << err << std::endl;
        sendErrorResponse(client_fd, RPC_METHOD_NOT_FOUND, err);
        close(client_fd);
        return;
    }

    google::protobuf::Service* service = service_it->second.service;
    const google::protobuf::MethodDescriptor* method = method_it->second;

    // 创建 protobuf 请求和响应对象
    google::protobuf::Message* request = service->GetRequestPrototype(method).New();
    google::protobuf::Message* response = service->GetResponsePrototype(method).New();

    // 解析 protobuf 请求对象
    if (!request->ParseFromString(request_body)) {
        std::string err = "parse request body failed";
        std::cerr << err << std::endl;
        sendErrorResponse(client_fd, RPC_PARSE_REQUEST_FAILED, err);
        delete request;
        delete response;
        close(client_fd);
        return;
    }

    // 调用业务方法
    google::protobuf::Closure* done = new SendResponseClosure(client_fd, response);

    service->CallMethod(method, nullptr, request, response, done);

    // 释放请求对象
    delete request;
}

} // namespace tinyrpc
