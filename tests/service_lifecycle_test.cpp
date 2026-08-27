#include "calculator.pb.h"

#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/registry/registry_server.h>
#include <tinyrpc/server/rpc_server.h>

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

// 提供服务生命周期测试所需的计算方法
class CalculatorServiceImpl : public tinyrpc::CalculatorService {
public:
    void Add(google::protobuf::RpcController*,
             const tinyrpc::AddRequest* request,
             tinyrpc::AddResponse* response,
             google::protobuf::Closure*) override {
        response->set_result(request->a() + request->b());
    }

    void Sub(google::protobuf::RpcController*,
             const tinyrpc::SubRequest* request,
             tinyrpc::SubResponse* response,
             google::protobuf::Closure*) override {
        response->set_result(request->a() - request->b());
    }
};

// 让内核分配一个当前可用的本地端口
uint16_t findFreePort() {
    const auto fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        close(fd);
        return 0;
    }

    socklen_t address_size = sizeof(address);
    if (getsockname(fd,
                    reinterpret_cast<sockaddr*>(&address),
                    &address_size) < 0) {
        close(fd);
        return 0;
    }

    const auto port = ntohs(address.sin_port);
    close(fd);
    return port;
}

// 等待指定网络端点开始接受连接
bool waitForEndpoint(const tinyrpc::Endpoint& endpoint) {
    for (int i = 0; i < 100; ++i) {
        if (tinyrpc::TcpSocket::connect(endpoint, 50)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

// 等待目标服务注册成功
bool waitForService(tinyrpc::RegistryClient& registry,
                    const std::string& service_name) {
    for (int i = 0; i < 100; ++i) {
        if (!registry.discoverServiceEndpoints(service_name).empty()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

} // namespace

int main() {
    // 为注册中心和 RPC 服务分配不同端口
    const auto registry_port = findFreePort();
    auto service_port = findFreePort();
    while (service_port == registry_port) {
        service_port = findFreePort();
    }
    if (registry_port == 0 || service_port == 0) {
        std::cerr << "allocate test port failed" << std::endl;
        return 1;
    }

    // 启动注册中心并等待监听就绪
    const tinyrpc::Endpoint registry_endpoint{"127.0.0.1", registry_port};
    tinyrpc::RegistryServer registry_server(registry_endpoint);
    std::thread registry_thread([&registry_server]() {
        registry_server.run();
    });

    if (!waitForEndpoint(registry_endpoint)) {
        registry_server.stop();
        registry_thread.join();
        std::cerr << "registry did not start" << std::endl;
        return 1;
    }

    // 注册并启动 RPC 服务
    const tinyrpc::Endpoint service_endpoint{"127.0.0.1", service_port};
    tinyrpc::RpcServer rpc_server(service_endpoint);
    rpc_server.setRegistry(registry_endpoint);
    rpc_server.setThreadPool(2, 16);
    CalculatorServiceImpl service;
    rpc_server.registerService(service);

    std::thread rpc_thread([&rpc_server]() {
        rpc_server.run();
    });

    // 等待服务注册信息可被发现
    tinyrpc::RegistryClient registry(registry_endpoint, 200);
    const auto service_name = service.GetDescriptor()->full_name();
    if (!waitForService(registry, service_name)) {
        rpc_server.stop();
        rpc_thread.join();
        registry_server.stop();
        registry_thread.join();
        std::cerr << "service was not registered" << std::endl;
        return 1;
    }

    // stop() 返回后，监听已关闭并且注册中心中的实例已主动注销
    rpc_server.stop();
    rpc_thread.join();
    const auto endpoints = registry.discoverServiceEndpoints(service_name);

    registry_server.stop();
    registry_thread.join();

    // 服务停止后不应残留注册信息
    if (!endpoints.empty()) {
        std::cerr << "service was not unregistered" << std::endl;
        return 1;
    }

    return 0;
}
