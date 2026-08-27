#include <tinyrpc/registry/registry_server.h>

#include <tinyrpc/common/tcp_listener.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/common/thread_pool.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <sys/socket.h>
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

// 注册中心每秒检查一次，连续 6 秒没有心跳就摘除实例
constexpr auto kCleanupInterval = std::chrono::seconds(1);
constexpr auto kHeartbeatTimeout = std::chrono::seconds(6);

} // namespace

// 配置注册中心监听地址
RegistryServer::RegistryServer(Endpoint listen_endpoint)
    : listen_endpoint_(std::move(listen_endpoint)) {}

// 停止后台任务并等待清理线程退出
RegistryServer::~RegistryServer() {
    stop();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
}

// 请求停止注册中心，并打断阻塞中的 accept
void RegistryServer::stop() {
    stop_requested_.store(true);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        cleanup_stopped_ = true;
    }
    cleanup_cv_.notify_all();

    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    if (listen_fd_ >= 0) {
        shutdown(listen_fd_, SHUT_RDWR);
    }
}

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
    if (command == "REGISTER" || command == "HEARTBEAT") {
        // 注册和心跳都采用 upsert：实例不存在时新增，存在时更新时间
        std::string service_name;
        std::string ip;
        uint16_t port = 0;

        if (iss >> service_name >> ip >> port) {
            std::lock_guard<std::mutex> lock(mutex_);
            auto& instances = service_endpoints_[service_name];
            const Endpoint endpoint{ip, port};
            const auto instance = std::find_if(
                instances.begin(), instances.end(),
                [&](const ServiceInstance& value) {
                    return value.endpoint == endpoint;
                });
            const auto now = std::chrono::steady_clock::now();
            if (instance == instances.end()) {
                instances.push_back({endpoint, now});
            } else {
                instance->last_heartbeat = now;
            }
            response = "OK\n";
            if (command == "REGISTER") {
                std::cout << "register service: " << service_name << " "
                          << ip << ":" << port << std::endl;
            }
        }
    } else if (command == "UNREGISTER") {
        // 主动注销指定服务实例；实例不存在时也视为成功
        std::string service_name;
        std::string ip;
        uint16_t port = 0;
        if (iss >> service_name >> ip >> port) {
            std::lock_guard<std::mutex> lock(mutex_);
            const Endpoint endpoint{ip, port};
            const auto service = service_endpoints_.find(service_name);
            if (service != service_endpoints_.end()) {
                auto& instances = service->second;
                instances.erase(
                    std::remove_if(
                        instances.begin(), instances.end(),
                        [&](const ServiceInstance& instance) {
                            return instance.endpoint == endpoint;
                        }),
                    instances.end());
                if (instances.empty()) {
                    service_endpoints_.erase(service);
                }
            }
            response = "OK\n";
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
                for (const auto& instance : it->second) {
                    oss << " " << instance.endpoint.ip << " "
                        << instance.endpoint.port;
                }
                oss << "\n";
                response = oss.str();
            }
        }
    }

    // 回写处理结果
    client_socket.sendAll(response);
}

// 后台清理长时间没有续约的服务实例
void RegistryServer::cleanupLoop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!cleanup_stopped_) {
        cleanup_cv_.wait_for(lock, kCleanupInterval, [this]() {
            return cleanup_stopped_;
        });
        if (cleanup_stopped_) {
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        for (auto service = service_endpoints_.begin();
             service != service_endpoints_.end();) {
            auto& instances = service->second;
            instances.erase(
                std::remove_if(
                    instances.begin(), instances.end(),
                    [&](const ServiceInstance& instance) {
                        return now - instance.last_heartbeat >
                               kHeartbeatTimeout;
                    }),
                instances.end());

            // 服务没有存活实例时一并删除服务名
            if (instances.empty()) {
                service = service_endpoints_.erase(service);
            } else {
                ++service;
            }
        }
    }
}

// 启动注册中心
void RegistryServer::run() {
    stop_requested_.store(false);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cleanup_stopped_ = false;
    }

    // 创建注册中心监听 socket
    auto listener = TcpListener::bind(listen_endpoint_);
    if (!listener) {
        std::cerr << "create registry socket failed" << std::endl;
        return;
    }

    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        listen_fd_ = listener->fd();
    }

    std::cout << "tinyrpc registry start at " << listen_endpoint_.ip << ":"
              << listen_endpoint_.port << std::endl;

    // 创建连接处理线程池
    ThreadPool thread_pool(kRegistryWorkerCount, kRegistryMaxQueueSize);

    // 独立线程定期摘除失去心跳的实例
    cleanup_thread_ = std::thread(&RegistryServer::cleanupLoop, this);

    // 循环接收客户端连接
    while (!stop_requested_.load()) {
        auto client_socket = listener->accept();
        if (!client_socket) {
            if (stop_requested_.load()) {
                break;
            }
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

    // 等待清理线程退出，确保 RegistryServer 可以安全析构
    cleanup_cv_.notify_all();
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    {
        std::lock_guard<std::mutex> lock(lifecycle_mutex_);
        listen_fd_ = -1;
    }
}

} // namespace tinyrpc
