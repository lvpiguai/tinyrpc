#include "calculator.pb.h"

#include <tinyrpc/client/rpc_channel.h>
#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/rpc_controller.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/registry/registry_server.h>
#include <tinyrpc/server/rpc_server.h>

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

// 提供基准测试使用的轻量计算方法
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

// 等待测试服务完成注册
bool waitForService(tinyrpc::RegistryClient& registry,
                    const std::string& service_name) {
    for (int i = 0; i < 200; ++i) {
        if (!registry.discoverServiceEndpoints(service_name).empty()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
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

// 获取已排序延迟样本的指定分位数
long long percentile(const std::vector<long long>& values, double ratio) {
    const auto index = static_cast<std::size_t>(
        ratio * static_cast<double>(values.size() - 1));
    return values[index];
}

} // namespace

int main(int argc, char* argv[]) {
    // 从命令行读取并发线程数和单线程调用次数
    const auto thread_count = argc > 1 ? std::max(1, std::atoi(argv[1])) : 8;
    const auto calls_per_thread =
        argc > 2 ? std::max(1, std::atoi(argv[2])) : 1000;

    // 为注册中心和 RPC 服务分配不同端口
    const auto registry_port = findFreePort();
    auto service_port = findFreePort();
    while (service_port == registry_port) {
        service_port = findFreePort();
    }
    if (registry_port == 0 || service_port == 0) {
        std::cerr << "allocate benchmark port failed" << std::endl;
        return 1;
    }

    // 启动进程内注册中心
    const tinyrpc::Endpoint registry_endpoint{"127.0.0.1", registry_port};
    tinyrpc::RegistryServer registry_server(registry_endpoint);
    std::thread registry_thread([&]() { registry_server.run(); });
    if (!waitForEndpoint(registry_endpoint)) {
        registry_server.stop();
        registry_thread.join();
        std::cerr << "benchmark registry did not start" << std::endl;
        return 1;
    }

    // 启动并注册基准测试服务
    CalculatorServiceImpl service;
    tinyrpc::RpcServer rpc_server(
        tinyrpc::Endpoint{"127.0.0.1", service_port});
    rpc_server.setRegistry(registry_endpoint);
    rpc_server.setThreadPool(static_cast<std::size_t>(thread_count), 4096);
    rpc_server.registerService(service);
    std::thread server_thread([&]() { rpc_server.run(); });

    tinyrpc::RegistryClient registry(registry_endpoint, 500);
    const auto service_name = service.GetDescriptor()->full_name();
    if (!waitForService(registry, service_name)) {
        rpc_server.stop();
        server_thread.join();
        registry_server.stop();
        registry_thread.join();
        std::cerr << "benchmark service did not start" << std::endl;
        return 1;
    }

    // 按并发线程数配置客户端连接池
    tinyrpc::RpcChannel channel(tinyrpc::RpcChannel::Mode::Registry,
                                registry_endpoint);
    channel.setMaxConnections(static_cast<std::size_t>(thread_count));
    channel.setTimeout(3000);
    tinyrpc::CalculatorService_Stub stub(&channel);

    const auto total_calls = thread_count * calls_per_thread;
    std::vector<long long> latencies(static_cast<std::size_t>(total_calls));
    std::atomic<int> failures{0};
    std::atomic<int> ready_threads{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> callers;

    // 并发调用并记录每次请求延迟
    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        callers.emplace_back([&, thread_index]() {
            ready_threads.fetch_add(1);
            while (!start.load()) {
                std::this_thread::yield();
            }

            for (int i = 0; i < calls_per_thread; ++i) {
                tinyrpc::AddRequest request;
                request.set_a(thread_index);
                request.set_b(i);
                tinyrpc::AddResponse response;
                tinyrpc::RpcController controller;

                const auto begin = std::chrono::steady_clock::now();
                stub.Add(&controller, &request, &response, nullptr);
                const auto elapsed =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - begin).count();
                const auto index = thread_index * calls_per_thread + i;
                latencies[static_cast<std::size_t>(index)] = elapsed;

                if (controller.Failed() ||
                    response.result() != thread_index + i) {
                    failures.fetch_add(1);
                }
            }
        });
    }

    // 等待全部线程就绪后开始计时
    while (ready_threads.load() != thread_count) {
        std::this_thread::yield();
    }
    const auto benchmark_begin = std::chrono::steady_clock::now();
    start.store(true);
    for (auto& caller : callers) {
        caller.join();
    }
    const auto benchmark_elapsed =
        std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                     benchmark_begin).count();

    // 停止服务并回收后台线程
    rpc_server.stop();
    server_thread.join();
    registry_server.stop();
    registry_thread.join();

    // 汇总吞吐量和延迟分位数
    std::sort(latencies.begin(), latencies.end());
    const auto qps = static_cast<double>(total_calls) / benchmark_elapsed;
    std::cout << "threads: " << thread_count << "\n"
              << "total calls: " << total_calls << "\n"
              << "failures: " << failures.load() << "\n"
              << std::fixed << std::setprecision(2)
              << "elapsed: " << benchmark_elapsed << " s\n"
              << "throughput: " << qps << " QPS\n"
              << "latency p50: " << percentile(latencies, 0.50) << " us\n"
              << "latency p95: " << percentile(latencies, 0.95) << " us\n"
              << "latency p99: " << percentile(latencies, 0.99) << " us"
              << std::endl;

    return failures.load() == 0 ? 0 : 1;
}
