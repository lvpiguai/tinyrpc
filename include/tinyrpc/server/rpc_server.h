#pragma once

#include <tinyrpc/common/endpoint.h>

#include <google/protobuf/service.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>

namespace tinyrpc {

class TcpSocket;

// RPC 服务端
class RpcServer {
public:
    // 配置服务端监听地址
    explicit RpcServer(Endpoint listen_endpoint);
    ~RpcServer();

    // 注册本地业务服务
    void registerService(google::protobuf::Service& service);

    // 设置注册中心地址
    void setRegistry(Endpoint registry_endpoint);

    // 设置 RPC 请求处理线程池参数
    void setThreadPool(size_t worker_count, size_t max_queue_size);

    // 启动 RPC 服务并监听指定地址
    void run();

    // 请求服务端优雅停止
    void stop();

private:
    // 处理完整请求帧并返回编码后的响应帧
    std::string handleRequest(const std::string& frame);

    // 启动和停止注册中心心跳线程
    void startHeartbeat();
    void stopHeartbeat();

    // 从注册中心注销当前服务端提供的全部服务
    void unregisterServices();

private:
    // 按服务全名保存业务实现，RpcServer 不拥有这些对象
    std::unordered_map<std::string, google::protobuf::Service*> services_;
    Endpoint listen_endpoint_;
    std::optional<Endpoint> registry_endpoint_;

    // 线程池配置
    size_t worker_count_ = 0;
    size_t max_queue_size_ = 1024;

    // 控制服务实例的周期性心跳
    std::mutex heartbeat_mutex_;
    std::condition_variable heartbeat_cv_;
    bool heartbeat_stopped_ = true;
    std::thread heartbeat_thread_;

    // stop() 通过 completion_fd 唤醒 Reactor
    std::atomic<bool> stop_requested_{false};
    std::mutex lifecycle_mutex_;
    int completion_fd_ = -1;
};

} // namespace tinyrpc
