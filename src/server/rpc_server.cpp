#include <tinyrpc/server/rpc_server.h>

#include <tinyrpc/common/protocol_codec.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/tcp_listener.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/common/tcp_frame_transport.h>
#include <tinyrpc/common/thread_pool.h>

#include <arpa/inet.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

namespace tinyrpc {

namespace {

// 单次 epoll_wait 最多返回的事件数
constexpr int kMaxEpollEvents = 128;

// TCP 帧长度字段固定为 4 字节
constexpr std::size_t kFrameSizeFieldSize = sizeof(uint32_t);

// Reactor 每次从单个连接读取的临时缓冲区大小
constexpr std::size_t kReadBufferSize = 8192;

// 心跳间隔小于注册中心的超时时间，避免正常实例被摘除
constexpr auto kHeartbeatInterval = std::chrono::seconds(2);

// 编码 RPC 响应帧，失败时返回空字符串
std::string encodeRpcResponse(const RpcResponse& response) {
    std::string frame;
    const auto status = protocol_codec::encode(response, frame);
    return status.ok() ? frame : std::string{};
}

// 构造并编码错误响应
std::string encodeErrorResponse(const std::string& error_message) {
    RpcResponse response;
    response.success = false;
    response.error_message = error_message;
    return encodeRpcResponse(response);
}

// 为 RPC 帧添加 4 字节网络序长度字段
std::string buildTransportData(const std::string& frame) {
    const auto network_frame_size =
        htonl(static_cast<uint32_t>(frame.size()));

    std::string data;
    data.reserve(kFrameSizeFieldSize + frame.size());
    data.append(reinterpret_cast<const char*>(&network_frame_size),
                kFrameSizeFieldSize);
    data.append(frame);
    return data;
}

} // namespace

RpcServer::ClientConnection::ClientConnection(TcpSocket value)
    : socket(std::move(value)) {}

// 清理连接状态以接收下一个请求
void RpcServer::ClientConnection::reset() {
    input.clear();
    frame_size.reset();
    output.clear();
    sent_size = 0;
}

// 非阻塞读取并组装完整请求帧
RpcServer::ReadFrameResult RpcServer::readAvailableFrame(
    ClientConnection& connection,
    std::string& frame) {
    std::array<char, kReadBufferSize> buffer{};

    while (true) {
        const auto received =
            connection.socket.recvSome(buffer.data(), buffer.size());
        if (received > 0) {
            // 保存本次读取的数据
            connection.input.append(buffer.data(),
                                    static_cast<std::size_t>(received));

            // 解析帧长度
            if (!connection.frame_size &&
                connection.input.size() >= kFrameSizeFieldSize) {
                uint32_t network_frame_size = 0;
                std::memcpy(&network_frame_size,
                            connection.input.data(),
                            kFrameSizeFieldSize);
                connection.frame_size = ntohl(network_frame_size);
                if (*connection.frame_size >
                    TcpFrameTransport::kMaxFrameSize) {
                    return ReadFrameResult::Closed;
                }
            }

            // 提取完整请求帧
            if (connection.frame_size &&
                connection.input.size() >=
                    kFrameSizeFieldSize + *connection.frame_size) {
                frame = connection.input.substr(kFrameSizeFieldSize,
                                                *connection.frame_size);
                return ReadFrameResult::Complete;
            }
            continue;
        }

        if (received == 0) {
            return ReadFrameResult::Closed;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return ReadFrameResult::Incomplete;
        }
        return ReadFrameResult::Closed;
    }
}

// 初始化服务端运行资源
bool RpcServer::initialize() {
    cleanup();
    stop_requested_.store(false);

    // 创建监听 socket
    listener_ = TcpListener::bind(listen_endpoint_);
    if (!listener_) {
        std::cerr << "create server socket failed" << std::endl;
        return false;
    }

    // 创建 epoll
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        std::cerr << "create epoll failed" << std::endl;
        cleanup();
        return false;
    }

    // 将监听 socket 加入 epoll
    epoll_event listen_event{};
    listen_event.events = EPOLLIN;
    listen_event.data.fd = listener_->fd();
    if (epoll_ctl(epoll_fd_,
                  EPOLL_CTL_ADD,
                  listener_->fd(),
                  &listen_event) < 0) {
        std::cerr << "add listener to epoll failed" << std::endl;
        cleanup();
        return false;
    }

    // 创建用于唤醒 Reactor 的 eventfd
    {
        std::lock_guard<std::mutex> lock(reactor_wakeup_mutex_);
        reactor_wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    }
    if (reactor_wakeup_fd_ < 0) {
        std::cerr << "create reactor wakeup eventfd failed" << std::endl;
        cleanup();
        return false;
    }

    // 将唤醒事件加入 epoll
    epoll_event wakeup_event{};
    wakeup_event.events = EPOLLIN;
    wakeup_event.data.fd = reactor_wakeup_fd_;
    if (epoll_ctl(epoll_fd_,
                  EPOLL_CTL_ADD,
                  reactor_wakeup_fd_,
                  &wakeup_event) < 0) {
        std::cerr << "add reactor wakeup eventfd to epoll failed" << std::endl;
        cleanup();
        return false;
    }

    // 确定工作线程数
    auto worker_count = worker_count_;
    if (worker_count == 0) {
        worker_count = std::thread::hardware_concurrency();
        if (worker_count == 0) {
            worker_count = 4;
        }
    }

    // 创建请求处理线程池
    thread_pool_ =
        std::make_unique<ThreadPool>(worker_count, max_queue_size_);
    return true;
}

// 唤醒 Reactor 事件循环
void RpcServer::wakeupReactor() {
    std::lock_guard<std::mutex> lock(reactor_wakeup_mutex_);
    if (reactor_wakeup_fd_ < 0) {
        return;
    }

    uint64_t one = 1;
    while (write(reactor_wakeup_fd_, &one, sizeof(one)) < 0 &&
           errno == EINTR) {
    }
}

// 处理工作线程完成的响应
void RpcServer::handleCompletedResponses() {
    // 取出本轮完成响应
    std::queue<CompletedResponse> ready_responses;
    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        ready_responses.swap(completed_responses_);
    }

    // 将响应关联到对应连接
    while (!ready_responses.empty()) {
        auto completed = std::move(ready_responses.front());
        ready_responses.pop();

        // 忽略已经关闭的连接
        const auto connection = connections_.find(completed.client_fd);
        if (connection == connections_.end()) {
            continue;
        }

        // 响应生成失败时关闭连接
        if (completed.frame.empty()) {
            connections_.erase(connection);
            continue;
        }

        // 保存等待发送的响应数据
        connection->second.output = buildTransportData(completed.frame);
        connection->second.sent_size = 0;

        // 等待连接可写
        epoll_event write_event{};
        write_event.events = EPOLLOUT | EPOLLRDHUP;
        write_event.data.fd = completed.client_fd;
        if (epoll_ctl(epoll_fd_,
                      EPOLL_CTL_ADD,
                      completed.client_fd,
                      &write_event) < 0) {
            connections_.erase(connection);
        }
    }
}

// 接收并监听客户端连接
void RpcServer::acceptConnection() {
    auto client_socket = listener_->accept();
    if (!client_socket) {
        std::cerr << "accept client failed" << std::endl;
        return;
    }
    if (!client_socket->setNonBlocking()) {
        return;
    }

    // 保存客户端连接
    const auto client_fd = client_socket->fd();
    connections_.emplace(client_fd,
                         ClientConnection(std::move(*client_socket)));

    // 将客户端连接加入 epoll
    epoll_event client_event{};
    client_event.events = EPOLLIN | EPOLLRDHUP;
    client_event.data.fd = client_fd;
    if (epoll_ctl(epoll_fd_,
                  EPOLL_CTL_ADD,
                  client_fd,
                  &client_event) < 0) {
        connections_.erase(client_fd);
    }
}

// 分发客户端事件
void RpcServer::handleClientEvent(int client_fd, uint32_t events) {
    if (connections_.find(client_fd) == connections_.end()) {
        return;
    }

    // 关闭异常或断开的连接
    if ((events & (EPOLLERR | EPOLLHUP)) != 0 ||
        ((events & EPOLLIN) == 0 && (events & EPOLLRDHUP) != 0)) {
        closeConnection(client_fd);
        return;
    }

    if ((events & EPOLLIN) != 0) {
        handleReadable(client_fd);
        return;
    }

    if ((events & EPOLLOUT) != 0) {
        handleWritable(client_fd);
    }
}

// 读取请求并提交线程池
void RpcServer::handleReadable(int client_fd) {
    const auto connection = connections_.find(client_fd);
    if (connection == connections_.end()) {
        return;
    }

    // 读取完整请求帧
    std::string frame;
    const auto read_result = readAvailableFrame(connection->second, frame);
    if (read_result == ReadFrameResult::Incomplete) {
        return;
    }
    if (read_result == ReadFrameResult::Closed) {
        closeConnection(client_fd);
        return;
    }

    // 请求处理期间暂停监听连接
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);

    // 在线程池中处理请求
    const auto submitted = thread_pool_->submit(
        [this, client_fd, frame = std::move(frame)]() mutable {
            auto response_frame = handleRequest(frame);
            {
                std::lock_guard<std::mutex> lock(completed_mutex_);
                completed_responses_.push(
                    {client_fd, std::move(response_frame)});
            }
            wakeupReactor();
        });
    if (!submitted) {
        std::cerr << "rpc thread pool queue is full, reject connection"
                  << std::endl;
        connections_.erase(connection);
    }
}

// 向客户端发送响应
void RpcServer::handleWritable(int client_fd) {
    const auto connection = connections_.find(client_fd);
    if (connection == connections_.end()) {
        return;
    }

    auto& client = connection->second;
    while (client.sent_size < client.output.size()) {
        const auto sent = client.socket.sendSome(
            client.output.data() + client.sent_size,
            client.output.size() - client.sent_size);
        if (sent > 0) {
            client.sent_size += static_cast<std::size_t>(sent);
            continue;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;
        }

        closeConnection(client_fd);
        return;
    }

    // 继续等待下一个请求
    client.reset();
    epoll_event read_event{};
    read_event.events = EPOLLIN | EPOLLRDHUP;
    read_event.data.fd = client_fd;
    if (epoll_ctl(epoll_fd_,
                  EPOLL_CTL_MOD,
                  client_fd,
                  &read_event) < 0) {
        connections_.erase(connection);
    }
}

// 关闭客户端连接
void RpcServer::closeConnection(int client_fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    connections_.erase(client_fd);
}

// 运行 Reactor 事件循环
void RpcServer::eventLoop() {
    std::array<epoll_event, kMaxEpollEvents> events{};

    while (!stop_requested_.load()) {
        // 等待文件描述符就绪
        const auto event_count =
            epoll_wait(epoll_fd_,
                       events.data(),
                       static_cast<int>(events.size()),
                       -1);
        if (event_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "epoll_wait failed: " << std::strerror(errno)
                      << std::endl;
            stop_requested_.store(true);
            break;
        }

        // 分发本轮就绪事件
        for (int i = 0; i < event_count; ++i) {
            const auto ready_fd = events[i].data.fd;
            const auto ready_events = events[i].events;

            // 处理 Reactor 唤醒事件
            if (ready_fd == reactor_wakeup_fd_) {
                // 清空 eventfd 唤醒计数
                uint64_t wakeup_count = 0;
                while (read(reactor_wakeup_fd_,
                            &wakeup_count,
                            sizeof(wakeup_count)) < 0 &&
                       errno == EINTR) {
                }

                // 收到停止请求时退出事件循环
                if (stop_requested_.load()) {
                    break;
                }
                handleCompletedResponses();
                continue;
            }

            // 接收客户端连接
            if (ready_fd == listener_->fd()) {
                acceptConnection();
                continue;
            }

            // 处理客户端读写事件
            handleClientEvent(ready_fd, ready_events);
        }
    }
}

// 清理服务端运行资源
void RpcServer::cleanup() {
    // 等待请求处理结束
    thread_pool_.reset();
    connections_.clear();
    listener_.reset();

    // 清空未发送响应
    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        std::queue<CompletedResponse> empty;
        completed_responses_.swap(empty);
    }

    // 关闭 Reactor 唤醒事件
    {
        std::lock_guard<std::mutex> lock(reactor_wakeup_mutex_);
        if (reactor_wakeup_fd_ >= 0) {
            close(reactor_wakeup_fd_);
            reactor_wakeup_fd_ = -1;
        }
    }

    // 关闭 epoll
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

// 配置服务端监听地址
RpcServer::RpcServer(Endpoint listen_endpoint)
    : listen_endpoint_(std::move(listen_endpoint)) {}

// 析构前触发服务端停止流程
RpcServer::~RpcServer() {
    stop();
    cleanup();
}

// 注册本地业务服务
void RpcServer::registerService(google::protobuf::Service& service) {
    // 获取 service 描述
    const auto* service_desc = service.GetDescriptor();

    // 按 service 全名注册
    services_[service_desc->full_name()] = &service;
}

// 配置注册中心地址
void RpcServer::setRegistry(Endpoint registry_endpoint) {
    registry_endpoint_ = std::move(registry_endpoint);
}

// 配置请求处理线程池
void RpcServer::setThreadPool(size_t worker_count, size_t max_queue_size) {
    worker_count_ = worker_count;
    max_queue_size_ = max_queue_size;
}

// 周期性向注册中心续约当前服务端提供的全部服务
void RpcServer::startHeartbeat() {
    if (!registry_endpoint_) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(heartbeat_mutex_);
        heartbeat_stopped_ = false;
    }

    heartbeat_thread_ = std::thread([this]() {
        RegistryClient registry(*registry_endpoint_);
        std::unique_lock<std::mutex> lock(heartbeat_mutex_);
        while (!heartbeat_cv_.wait_for(
            lock, kHeartbeatInterval,
            [this]() { return heartbeat_stopped_; })) {
            // 网络操作可能阻塞，执行时不持有心跳状态锁
            lock.unlock();
            for (const auto& item : services_) {
                if (!registry.sendHeartbeat(item.first, listen_endpoint_)) {
                    std::cerr << "heartbeat service failed: " << item.first
                              << std::endl;
                }
            }
            lock.lock();
        }
    });
}

// 通知心跳线程退出并等待其结束
void RpcServer::stopHeartbeat() {
    {
        std::lock_guard<std::mutex> lock(heartbeat_mutex_);
        heartbeat_stopped_ = true;
    }
    heartbeat_cv_.notify_all();
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
}

// 向注册中心发布服务
void RpcServer::registerServices() {
    if (!registry_endpoint_) {
        return;
    }

    RegistryClient registry(*registry_endpoint_);
    for (const auto& item : services_) {
        if (!registry.registerServiceEndpoint(item.first, listen_endpoint_)) {
            std::cerr << "register service to registry failed: " << item.first
                      << std::endl;
        }
    }
}

// 从注册中心主动删除当前服务端提供的全部服务
void RpcServer::unregisterServices() {
    if (!registry_endpoint_) {
        return;
    }

    RegistryClient registry(*registry_endpoint_);
    for (const auto& item : services_) {
        if (!registry.unregisterServiceEndpoint(item.first,
                                                listen_endpoint_)) {
            std::cerr << "unregister service failed: " << item.first
                      << std::endl;
        }
    }
}

// 设置停止标记，并使用 eventfd 唤醒阻塞中的 epoll_wait
void RpcServer::stop() {
    stop_requested_.store(true);
    stopHeartbeat();
    wakeupReactor();
}

// 启动 RPC 服务端
void RpcServer::run() {
    if (!initialize()) {
        return;
    }

    registerServices();
    startHeartbeat();
    eventLoop();
    stopHeartbeat();
    cleanup();
    unregisterServices();
}

// 处理完整请求帧并生成响应帧，不执行网络收发
std::string RpcServer::handleRequest(const std::string& frame) {
    // 解码 RPC 请求帧
    RpcRequest rpc_request;
    auto status = protocol_codec::decode(frame, rpc_request);
    if (!status.ok()) {
        return {};
    }

    // 读取目标服务和方法
    const auto& service_name = rpc_request.service_name;
    const auto& method_name = rpc_request.method_name;

    // 查找目标服务
    const auto service_it = services_.find(service_name);
    if (service_it == services_.end()) {
        return encodeErrorResponse("service not found: " + service_name);
    }

    // 查找目标方法
    auto* service = service_it->second;
    const auto* method =
        service->GetDescriptor()->FindMethodByName(method_name);
    if (method == nullptr) {
        return encodeErrorResponse("method not found: " + method_name);
    }

    // 创建 protobuf 请求和响应对象
    std::unique_ptr<google::protobuf::Message> request(
        service->GetRequestPrototype(method).New());
    std::unique_ptr<google::protobuf::Message> response(
        service->GetResponsePrototype(method).New());

    // 解析 protobuf 请求对象
    if (!request->ParseFromString(rpc_request.serialized_body)) {
        return encodeErrorResponse("parse request body failed");
    }

    // 同步调用业务方法
    service->CallMethod(method,
                        nullptr,
                        request.get(),
                        response.get(),
                        nullptr);

    // 序列化 protobuf 响应对象
    std::string serialized_response_body;
    if (!response->SerializeToString(&serialized_response_body)) {
        return encodeErrorResponse("serialize response failed");
    }

    // 构造并编码成功响应
    RpcResponse rpc_response;
    rpc_response.serialized_body = std::move(serialized_response_body);
    return encodeRpcResponse(rpc_response);
}

} // namespace tinyrpc
