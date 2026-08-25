#include "calculator.pb.h"

#include <tinyrpc/client/rpc_channel.h>
#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/rpc_controller.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/registry/registry_server.h>
#include <tinyrpc/server/rpc_server.h>

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

class CalculatorServiceImpl : public tinyrpc::CalculatorService {
public:
    void Add(google::protobuf::RpcController*,
             const tinyrpc::AddRequest* request,
             tinyrpc::AddResponse* response,
             google::protobuf::Closure*) override {
        add_calls.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        response->set_result(request->a() + request->b());
    }

    void Sub(google::protobuf::RpcController*,
             const tinyrpc::SubRequest* request,
             tinyrpc::SubResponse* response,
             google::protobuf::Closure*) override {
        response->set_result(request->a() - request->b());
    }

    std::atomic<int> add_calls{0};
};

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

bool waitForInstances(tinyrpc::RegistryClient& registry,
                      const std::string& service_name,
                      std::size_t expected_count) {
    for (int i = 0; i < 200; ++i) {
        if (registry.discoverServiceEndpoints(service_name).size() ==
            expected_count) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

bool waitForEndpoint(const tinyrpc::Endpoint& endpoint) {
    for (int i = 0; i < 100; ++i) {
        if (tinyrpc::TcpSocket::connect(endpoint.ip, endpoint.port, 50)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

} // namespace

int main() {
    const auto registry_port = findFreePort();
    auto first_service_port = findFreePort();
    while (first_service_port == registry_port) {
        first_service_port = findFreePort();
    }
    auto second_service_port = findFreePort();
    while (second_service_port == registry_port ||
           second_service_port == first_service_port) {
        second_service_port = findFreePort();
    }
    if (registry_port == 0 || first_service_port == 0 ||
        second_service_port == 0) {
        std::cerr << "allocate test port failed" << std::endl;
        return 1;
    }

    const tinyrpc::Endpoint registry_endpoint{"127.0.0.1", registry_port};
    tinyrpc::RegistryServer registry_server(registry_endpoint);
    std::thread registry_thread([&]() { registry_server.run(); });
    if (!waitForEndpoint(registry_endpoint)) {
        registry_server.stop();
        registry_thread.join();
        std::cerr << "registry did not start" << std::endl;
        return 1;
    }

    CalculatorServiceImpl first_service;
    CalculatorServiceImpl second_service;
    tinyrpc::RpcServer first_server(
        tinyrpc::Endpoint{"127.0.0.1", first_service_port});
    tinyrpc::RpcServer second_server(
        tinyrpc::Endpoint{"127.0.0.1", second_service_port});
    first_server.setRegistry(registry_endpoint);
    second_server.setRegistry(registry_endpoint);
    first_server.setThreadPool(4, 128);
    second_server.setThreadPool(4, 128);
    first_server.registerService(first_service);
    second_server.registerService(second_service);

    std::thread first_server_thread([&]() { first_server.run(); });
    std::thread second_server_thread([&]() { second_server.run(); });

    tinyrpc::RegistryClient registry(registry_endpoint, 200);
    const auto service_name = first_service.GetDescriptor()->full_name();
    if (!waitForInstances(registry, service_name, 2)) {
        first_server.stop();
        second_server.stop();
        first_server_thread.join();
        second_server_thread.join();
        registry_server.stop();
        registry_thread.join();
        std::cerr << "service instances did not register" << std::endl;
        return 1;
    }

    constexpr int kThreadCount = 8;
    constexpr int kCallsPerThread = 50;
    tinyrpc::RpcChannel channel(tinyrpc::RpcChannel::Mode::Registry,
                                registry_endpoint);
    channel.setMaxConnections(4);
    channel.setTimeout(1000);
    tinyrpc::CalculatorService_Stub stub(&channel);

    std::atomic<int> ready_threads{0};
    std::atomic<bool> start{false};
    std::atomic<int> failures{0};
    std::vector<std::thread> callers;
    for (int thread_index = 0; thread_index < kThreadCount; ++thread_index) {
        callers.emplace_back([&, thread_index]() {
            ready_threads.fetch_add(1);
            while (!start.load()) {
                std::this_thread::yield();
            }

            for (int i = 0; i < kCallsPerThread; ++i) {
                tinyrpc::AddRequest request;
                request.set_a(thread_index);
                request.set_b(i);
                tinyrpc::AddResponse response;
                tinyrpc::RpcController controller;
                stub.Add(&controller, &request, &response, nullptr);
                if (controller.Failed() ||
                    response.result() != thread_index + i) {
                    failures.fetch_add(1);
                }
            }
        });
    }

    while (ready_threads.load() != kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true);
    for (auto& caller : callers) {
        caller.join();
    }

    const auto used_both_instances = first_service.add_calls.load() > 0 &&
                                     second_service.add_calls.load() > 0;

    // 一个实例停止并主动注销后，新通道应只发现并调用存活实例
    first_server.stop();
    first_server_thread.join();
    const auto one_instance_left = waitForInstances(registry,
                                                    service_name,
                                                    1);

    tinyrpc::RpcChannel failover_channel(
        tinyrpc::RpcChannel::Mode::Registry, registry_endpoint);
    failover_channel.setTimeout(1000);
    tinyrpc::CalculatorService_Stub failover_stub(&failover_channel);
    tinyrpc::AddRequest request;
    request.set_a(20);
    request.set_b(22);
    tinyrpc::AddResponse response;
    tinyrpc::RpcController controller;
    failover_stub.Add(&controller, &request, &response, nullptr);

    second_server.stop();
    second_server_thread.join();
    registry_server.stop();
    registry_thread.join();

    if (failures.load() != 0) {
        std::cerr << "concurrent rpc calls failed: " << failures.load()
                  << std::endl;
        return 1;
    }
    if (!used_both_instances) {
        std::cerr << "round robin did not use both instances" << std::endl;
        return 1;
    }
    if (!one_instance_left || controller.Failed() || response.result() != 42) {
        std::cerr << "rpc failover failed" << std::endl;
        return 1;
    }

    return 0;
}
