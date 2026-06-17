#include "rpc_provider.h"

#include "rpc_header.pb.h"
#include "tcp_socket.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

namespace tinyrpc {

// 响应回写回调
class SendResponseClosure : public google::protobuf::Closure {
public:
    SendResponseClosure(int client_fd, google::protobuf::Message* response)
        : client_fd_(client_fd), response_(response) {}

    void Run() override {
        // 序列化响应
        std::string response_str;

        if (!response_->SerializeToString(&response_str)) {
            std::cerr << "serialize response failed" << std::endl;
            close(client_fd_);
            delete response_;
            delete this;
            return;
        }

        // 发送长度和响应体
        uint32_t response_size = static_cast<uint32_t>(response_str.size());
        uint32_t net_response_size = htonl(response_size);

        TcpSocket::sendAll(client_fd_, reinterpret_cast<char*>(&net_response_size), sizeof(net_response_size));
        TcpSocket::sendAll(client_fd_, response_str);

        // 关闭连接并释放对象
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

void RpcProvider::run(const std::string& ip, uint16_t port) {
    // 创建监听 socket
    int listen_fd = TcpSocket::createServerSocket(ip, port);
    if (listen_fd < 0) {
        std::cerr << "create server socket failed" << std::endl;
        return;
    }

    std::cout << "tinyrpc server start at " << ip << ":" << port << std::endl;

    // 循环处理客户端连接
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(listen_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        std::thread(&RpcProvider::handleClient, this, client_fd).detach();
    }
}

void RpcProvider::handleClient(int client_fd) {
    // 读取头部长度
    uint32_t net_header_size = 0;

    if (!TcpSocket::recvAll(client_fd, reinterpret_cast<char*>(&net_header_size), sizeof(net_header_size))) {
        std::cerr << "recv header size failed" << std::endl;
        close(client_fd);
        return;
    }

    uint32_t header_size = ntohl(net_header_size);

    // 读取并解析头部
    std::string header_str;
    if (!TcpSocket::recvAll(client_fd, header_str, header_size)) {
        std::cerr << "recv header failed" << std::endl;
        close(client_fd);
        return;
    }

    RpcHeader header;
    if (!header.ParseFromString(header_str)) {
        std::cerr << "parse rpc header failed" << std::endl;
        close(client_fd);
        return;
    }

    std::string service_name = header.service_name();
    std::string method_name = header.method_name();
    uint32_t args_size = header.args_size();

    // 读取请求参数
    std::string args_str;
    if (!TcpSocket::recvAll(client_fd, args_str, args_size)) {
        std::cerr << "recv args failed" << std::endl;
        close(client_fd);
        return;
    }

    // 查找 service 和 method
    auto service_it = services_.find(service_name);
    if (service_it == services_.end()) {
        std::cerr << "service not found: " << service_name << std::endl;
        close(client_fd);
        return;
    }

    auto method_it = service_it->second.methods.find(method_name);
    if (method_it == service_it->second.methods.end()) {
        std::cerr << "method not found: " << method_name << std::endl;
        close(client_fd);
        return;
    }

    google::protobuf::Service* service = service_it->second.service;
    const google::protobuf::MethodDescriptor* method = method_it->second;

    // 创建请求和响应对象
    google::protobuf::Message* request = service->GetRequestPrototype(method).New();
    google::protobuf::Message* response = service->GetResponsePrototype(method).New();

    // 反序列化请求参数
    if (!request->ParseFromString(args_str)) {
        std::cerr << "parse request args failed" << std::endl;
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
