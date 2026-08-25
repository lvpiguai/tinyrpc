#include <tinyrpc/registry/registry_server.h>

#include <tinyrpc/common/tcp_listener.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/common/thread_pool.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace tinyrpc {

namespace {

// 单条注册中心命令最大长度
constexpr std::size_t kMaxRegistryLineSize = 1024;

// 注册中心工作线程数和等待队列容量
constexpr std::size_t kRegistryWorkerCount = 4;
constexpr std::size_t kRegistryMaxQueueSize = 1024;

} // namespace

// 配置注册中心监听地址
RegistryServer::RegistryServer(Endpoint listen_endpoint)
    : listen_endpoint_(std::move(listen_endpoint)) {}

// 处理单个注册中心连接
void RegistryServer::handleClient(TcpSocket client_socket) {
    // 接收一行命令
    const auto line = client_socket.recvLine(kMaxRegistryLineSize);
    if (!line) {
        return;
    }

    // 解析命令类型
    std::istringstream iss(*line);
    std::string command;
    iss >> command;

    std::string response = "ERROR\n";
    if (command == "REGISTER") {
        // 注册服务端点
        std::string service_name;
        std::string ip;
        uint16_t port = 0;

        if (iss >> service_name >> ip >> port) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& endpoints = service_endpoints_[service_name];
            const auto duplicate = std::find_if(
                endpoints.begin(), endpoints.end(),
                [&](const Endpoint& endpoint) {
                    return endpoint.ip == ip && endpoint.port == port;
                });
            if (duplicate == endpoints.end()) {
                endpoints.push_back(Endpoint{ip, port});
            }
            response = "OK\n";
            std::cout << "register service: " << service_name << " "
                      << ip << ":" << port << std::endl;
        }
    } else if (command == "DISCOVER") {
        // 发现服务端点
        std::string service_name;
        if (iss >> service_name) {
            std::lock_guard<std::mutex> lock(mutex_);
            const auto it = service_endpoints_.find(service_name);
            if (it == service_endpoints_.end()) {
                response = "NOT_FOUND\n";
            } else {
                // 返回：FOUND <数量> <ip1> <port1> ...
                std::ostringstream oss;
                oss << "FOUND " << it->second.size();
                for (const auto& endpoint : it->second) {
                    oss << " " << endpoint.ip << " " << endpoint.port;
                }
                oss << "\n";
                response = oss.str();
            }
        }
    }

    // 回写处理结果
    client_socket.sendAll(response);
}

// 启动注册中心
void RegistryServer::run() {
    // 创建注册中心监听 socket
    auto listener = TcpListener::bind(listen_endpoint_.ip,
                                      listen_endpoint_.port);
    if (!listener) {
        std::cerr << "create registry socket failed" << std::endl;
        return;
    }

    std::cout << "tinyrpc registry start at " << listen_endpoint_.ip << ":"
              << listen_endpoint_.port << std::endl;

    // 创建连接处理线程池
    ThreadPool thread_pool(kRegistryWorkerCount, kRegistryMaxQueueSize);

    // 循环接收客户端连接
    while (true) {
        auto client_socket = listener->accept();
        if (!client_socket) {
            std::cerr << "accept client failed" << std::endl;
            continue;
        }

        // std::function 需要可复制任务，用 shared_ptr 保存可移动 Socket
        auto shared_socket = std::make_shared<TcpSocket>(std::move(*client_socket));

        // 将注册中心连接交给线程池处理
        const auto submitted = thread_pool.submit([this, shared_socket]() {
            handleClient(std::move(*shared_socket));
        });
        if (!submitted) {
            std::cerr << "registry thread pool queue is full, reject connection"
                      << std::endl;
        }
    }
}

} // namespace tinyrpc
