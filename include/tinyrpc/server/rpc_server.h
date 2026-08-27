#pragma once

#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/common/tcp_listener.h>
#include <tinyrpc/common/tcp_socket.h>

#include <google/protobuf/service.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

namespace tinyrpc {

class ThreadPool;

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

    // 请求服务端停止并唤醒运行线程
    void stop();

private:
    // 客户端连接及其收发状态
    struct ClientConnection {
        explicit ClientConnection(TcpSocket socket);

        void reset();

        TcpSocket socket;                         // 客户端连接
        std::string input;                        // 已读取的请求数据
        std::optional<std::size_t> frame_size;    // 请求帧长度
        std::string output;                       // 等待发送的响应数据
        std::size_t sent_size = 0;                // 已发送的响应长度
    };

    // 工作线程完成的响应
    struct CompletedResponse {
        int client_fd;
        std::string frame;
    };

    enum class ReadFrameResult {
        Incomplete, // 请求帧尚未收完整
        Complete,   // 请求帧读取完成
        Closed      // 连接关闭或请求帧非法
    };

    // 初始化和清理运行资源
    bool initialize();
    void cleanup();

    // 运行并唤醒 Reactor 事件循环
    void eventLoop();
    void wakeupReactor();

    // 处理 Reactor 事件
    void handleCompletedResponses();
    void acceptConnection();
    void handleClientEvent(int client_fd, uint32_t events);
    void handleReadable(int client_fd);
    void handleWritable(int client_fd);
    void closeConnection(int client_fd);

    // 读取并组装完整请求帧
    ReadFrameResult readAvailableFrame(ClientConnection& connection,
                                       std::string& frame);

    // 处理完整请求帧并返回编码后的响应帧
    std::string handleRequest(const std::string& frame);

    // 向注册中心发布服务
    void registerServices();

    // 启动和停止注册中心心跳线程
    void startHeartbeat();
    void stopHeartbeat();

    // 从注册中心注销当前服务端提供的全部服务
    void unregisterServices();

private:
    // 按服务全名保存业务实现，RpcServer 不拥有这些对象
    std::unordered_map<std::string, google::protobuf::Service*> services_;
    Endpoint listen_endpoint_;                    // 服务端监听地址
    std::optional<Endpoint> registry_endpoint_;   // 可选注册中心地址

    // 线程池配置
    size_t worker_count_ = 0;
    size_t max_queue_size_ = 1024;

    // 控制服务实例的周期性心跳
    std::mutex heartbeat_mutex_;
    std::condition_variable heartbeat_cv_;
    bool heartbeat_stopped_ = true;
    std::thread heartbeat_thread_;

    // Reactor 运行资源
    std::optional<TcpListener> listener_;
    int epoll_fd_ = -1;
    int reactor_wakeup_fd_ = -1;
    std::unique_ptr<ThreadPool> thread_pool_;
    std::unordered_map<int, ClientConnection> connections_;
    std::mutex completed_mutex_;
    std::queue<CompletedResponse> completed_responses_;

    // 控制服务停止
    std::atomic<bool> stop_requested_{false};
    std::mutex reactor_wakeup_mutex_;
};

} // namespace tinyrpc
