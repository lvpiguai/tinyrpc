#pragma once

#include <tinyrpc/common/endpoint.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace tinyrpc {

class TcpSocket;

// 注册中心服务端，负责服务端点注册与发现
class RegistryServer {
public:
    // 配置注册中心监听地址
    explicit RegistryServer(Endpoint listen_endpoint);
    ~RegistryServer();

    // 启动注册中心
    void run();

    // 请求注册中心停止，并唤醒阻塞中的 accept
    void stop();

private:
    struct ServiceInstance {
        Endpoint endpoint;
        std::chrono::steady_clock::time_point last_heartbeat;
    };

    // 处理单个注册中心连接
    void handleClient(TcpSocket client_socket);

    // 定期删除超过心跳期限的实例
    void cleanupLoop();

    // 注册中心监听地址
    Endpoint listen_endpoint_;

    // 保存每个服务注册的全部实例
    std::unordered_map<std::string, std::vector<ServiceInstance>>
        service_endpoints_;

    // 保护服务端点映射和清理线程停止标记
    std::mutex mutex_;
    std::condition_variable cleanup_cv_;
    bool cleanup_stopped_ = false;
    std::thread cleanup_thread_;

    // 监听 socket 生命周期状态
    std::atomic<bool> stop_requested_{false};
    std::mutex lifecycle_mutex_;
    int listen_fd_ = -1;
};

} // namespace tinyrpc
