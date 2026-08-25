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

// 保存尚未形成完整请求帧的连接及其读取状态
struct ClientConnection {
    explicit ClientConnection(TcpSocket value)
        : socket(std::move(value)) {}

    TcpSocket socket;                         // 连接所有权
    std::string input;                        // 已读取但尚未处理的数据
    std::optional<std::size_t> frame_size;    // 长度字段解析后的帧大小
    std::string output;                       // 等待发送的传输层数据
    std::size_t sent_size = 0;                // 已发送字节数

    // 响应完成后清理状态，以接收同一连接的下一个请求
    void reset() {
        input.clear();
        frame_size.reset();
        output.clear();
        sent_size = 0;
    }
};

// 工作线程完成业务处理后交给 Reactor 的结果
struct CompletedResponse {
    int client_fd;
    std::string frame;
};

// Reactor 尝试读取一帧后的结果
enum class ReadFrameResult {
    Incomplete, // 当前数据不足，继续等待 EPOLLIN
    Complete,   // 已获得完整帧，可以提交线程池
    Closed      // 对端关闭、网络错误或帧长度非法
};

// 非阻塞读取当前已有数据，并尝试组装一个完整帧
ReadFrameResult readAvailableFrame(ClientConnection& client,
                                   std::string& frame) {
    std::array<char, kReadBufferSize> buffer{};

    while (true) {
        // 非阻塞 socket 只读取内核缓冲区中当前已有的数据
        const auto received = client.socket.recvSome(buffer.data(),
                                                     buffer.size());
        if (received > 0) {
            // 本次数据可能只是半包，先追加到连接缓冲区
            client.input.append(buffer.data(),
                                static_cast<std::size_t>(received));

            // 收齐 4 字节长度字段后，得到完整帧应有的大小
            if (!client.frame_size &&
                client.input.size() >= kFrameSizeFieldSize) {
                uint32_t network_frame_size = 0;
                std::memcpy(&network_frame_size,
                            client.input.data(),
                            kFrameSizeFieldSize);
                client.frame_size = ntohl(network_frame_size);
                if (*client.frame_size > TcpFrameTransport::kMaxFrameSize) {
                    return ReadFrameResult::Closed;
                }
            }

            // 长度字段和完整帧都已到达时提取 RPC 帧
            if (client.frame_size &&
                client.input.size() >=
                    kFrameSizeFieldSize + *client.frame_size) {
                frame = client.input.substr(kFrameSizeFieldSize,
                                            *client.frame_size);
                return ReadFrameResult::Complete;
            }
            continue;
        }

        // recv 返回 0 表示对端已关闭连接
        if (received == 0) {
            return ReadFrameResult::Closed;
        }
        // 被信号中断时重新尝试读取
        if (errno == EINTR) {
            continue;
        }
        // EAGAIN 表示当前数据已读完，并非网络错误
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return ReadFrameResult::Incomplete;
        }
        return ReadFrameResult::Closed;
    }
}

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

// 配置服务端监听地址
RpcServer::RpcServer(Endpoint listen_endpoint)
    : listen_endpoint_(std::move(listen_endpoint)) {}

RpcServer::~RpcServer() {
    stopHeartbeat();
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
                if (!registry.heartbeatServiceEndpoint(item.first,
                                                       listen_endpoint_)) {
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

// 启动 RPC 服务端
void RpcServer::run() {
    // 创建监听 socket
    auto listener = TcpListener::bind(listen_endpoint_.ip,
                                      listen_endpoint_.port);
    if (!listener) {
        std::cerr << "create server socket failed" << std::endl;
        return;
    }

    // 确定工作线程数，0 表示根据硬件自动选择
    auto worker_count = worker_count_;
    if (worker_count == 0) {
        worker_count = std::thread::hardware_concurrency();
        if (worker_count == 0) {
            // 无法获取硬件并发数时使用默认值
            worker_count = 4;
        }
    }

    // 启用注册中心时，发布当前服务端提供的全部服务
    if (registry_endpoint_) {
        RegistryClient registry(*registry_endpoint_);
        for (const auto& item : services_) {
            if (!registry.registerServiceEndpoint(item.first,
                                                  listen_endpoint_)) {
                std::cerr << "register service to registry failed: " << item.first << std::endl;
            }
        }
    }

    // 创建 epoll，并监听服务端 socket
    const auto epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        std::cerr << "create epoll failed" << std::endl;
        return;
    }

    const auto listen_fd = listener->fd();

    // EPOLLIN 表示监听 socket 上存在待接收连接
    epoll_event listen_event{};
    listen_event.events = EPOLLIN;
    listen_event.data.fd = listen_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &listen_event) < 0) {
        std::cerr << "add listener to epoll failed" << std::endl;
        close(epoll_fd);
        return;
    }

    // eventfd 用于让工作线程唤醒阻塞在 epoll_wait 的 Reactor
    const auto completion_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (completion_fd < 0) {
        std::cerr << "create completion eventfd failed" << std::endl;
        close(epoll_fd);
        return;
    }

    epoll_event completion_event{};
    completion_event.events = EPOLLIN;
    completion_event.data.fd = completion_fd;
    if (epoll_ctl(epoll_fd,
                  EPOLL_CTL_ADD,
                  completion_fd,
                  &completion_event) < 0) {
        std::cerr << "add completion eventfd to epoll failed" << std::endl;
        close(completion_fd);
        close(epoll_fd);
        return;
    }

    // Reactor 线程持有尚未交给工作线程的客户端连接
    std::unordered_map<int, ClientConnection> clients;

    // 工作线程写入完成队列，Reactor 被 eventfd 唤醒后取出
    std::mutex completed_mutex;
    std::queue<CompletedResponse> completed_responses;

    // 后创建线程池，退出时先等待工作线程，再销毁完成队列
    auto thread_pool =
        std::make_unique<ThreadPool>(worker_count, max_queue_size_);
    std::array<epoll_event, kMaxEpollEvents> events{};

    // 网络初始化完成后开始周期性续约服务实例
    startHeartbeat();

    // 等待监听 socket 或客户端 socket 就绪
    while (true) {
        const auto event_count = epoll_wait(epoll_fd,
                                            events.data(),
                                            static_cast<int>(events.size()),
                                            -1);
        if (event_count < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "epoll_wait failed: " << std::strerror(errno)
                      << std::endl;
            stopHeartbeat();
            thread_pool.reset();
            close(completion_fd);
            close(epoll_fd);
            return;
        }

        for (int i = 0; i < event_count; ++i) {
            // epoll 通过 data.fd 告知本次就绪的文件描述符
            const auto ready_fd = events[i].data.fd;
            const auto ready_events = events[i].events;

            // 处理工作线程已经生成的响应帧
            if (ready_fd == completion_fd) {
                uint64_t completed_count = 0;
                while (read(completion_fd,
                            &completed_count,
                            sizeof(completed_count)) < 0 &&
                       errno == EINTR) {
                }

                std::queue<CompletedResponse> ready_responses;
                {
                    std::lock_guard<std::mutex> lock(completed_mutex);
                    ready_responses.swap(completed_responses);
                }

                while (!ready_responses.empty()) {
                    auto completed = std::move(ready_responses.front());
                    ready_responses.pop();

                    const auto completed_client =
                        clients.find(completed.client_fd);
                    if (completed_client == clients.end()) {
                        continue;
                    }
                    if (completed.frame.empty()) {
                        clients.erase(completed_client);
                        continue;
                    }

                    completed_client->second.output =
                        buildTransportData(completed.frame);
                    completed_client->second.sent_size = 0;

                    // 响应准备完成后重新加入 epoll，等待 socket 可写
                    epoll_event write_event{};
                    write_event.events = EPOLLOUT | EPOLLRDHUP;
                    write_event.data.fd = completed.client_fd;
                    if (epoll_ctl(epoll_fd,
                                  EPOLL_CTL_ADD,
                                  completed.client_fd,
                                  &write_event) < 0) {
                        clients.erase(completed_client);
                    }
                }
                continue;
            }

            // 监听 socket 可读时，接收一个连接并交给 epoll 监听
            if (ready_fd == listen_fd) {
                auto client_socket = listener->accept();
                if (!client_socket) {
                    std::cerr << "accept client failed" << std::endl;
                    continue;
                }

                if (!client_socket->setNonBlocking()) {
                    continue;
                }

                // clients 保存所有仍由 Reactor 管理的连接
                const auto client_fd = client_socket->fd();
                clients.emplace(client_fd,
                                ClientConnection(std::move(*client_socket)));

                epoll_event client_event{};
                // 监听可读事件及客户端关闭写端事件
                client_event.events = EPOLLIN | EPOLLRDHUP;
                client_event.data.fd = client_fd;
                if (epoll_ctl(epoll_fd,
                              EPOLL_CTL_ADD,
                              client_fd,
                              &client_event) < 0) {
                    clients.erase(client_fd);
                }
                continue;
            }

            const auto client_it = clients.find(ready_fd);
            if (client_it == clients.end()) {
                continue;
            }

            // 错误或无可读数据的断连直接关闭
            if ((ready_events & (EPOLLERR | EPOLLHUP)) != 0 ||
                ((ready_events & EPOLLIN) == 0 &&
                 (ready_events & EPOLLRDHUP) != 0)) {
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ready_fd, nullptr);
                clients.erase(client_it);
                continue;
            }

            if ((ready_events & EPOLLIN) != 0) {
                // Reactor 只读取当前已经到达的数据，不在这里等待
                std::string frame;
                const auto read_result = readAvailableFrame(client_it->second,
                                                            frame);
                if (read_result == ReadFrameResult::Incomplete) {
                    continue;
                }
                if (read_result == ReadFrameResult::Closed) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ready_fd, nullptr);
                    clients.erase(client_it);
                    continue;
                }

                // 业务处理期间暂时移出 epoll，连接仍由 Reactor 持有
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ready_fd, nullptr);
                const auto submitted = thread_pool->submit(
                    [this, ready_fd, frame = std::move(frame),
                     &completed_mutex, &completed_responses,
                     completion_fd]() mutable {
                        auto response_frame = handleRequest(frame);
                        {
                            std::lock_guard<std::mutex> lock(completed_mutex);
                            completed_responses.push(
                                {ready_fd, std::move(response_frame)});
                        }

                        // 写入计数器，使 completion_fd 产生 EPOLLIN
                        uint64_t one = 1;
                        while (write(completion_fd, &one, sizeof(one)) < 0 &&
                               errno == EINTR) {
                        }
                    });
                if (!submitted) {
                    std::cerr
                        << "rpc thread pool queue is full, reject connection"
                        << std::endl;
                    clients.erase(client_it);
                }
                continue;
            }

            if ((ready_events & EPOLLOUT) != 0) {
                auto& client = client_it->second;
                bool send_failed = false;

                // 尽可能发送，直到全部完成或内核发送缓冲区已满
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
                    if (sent < 0 &&
                        (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        break;
                    }
                    send_failed = true;
                    break;
                }

                if (send_failed) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ready_fd, nullptr);
                    clients.erase(client_it);
                    continue;
                }

                if (client.sent_size == client.output.size()) {
                    // 响应完成后保留连接，重新等待下一个请求
                    client.reset();
                    epoll_event read_event{};
                    read_event.events = EPOLLIN | EPOLLRDHUP;
                    read_event.data.fd = ready_fd;
                    if (epoll_ctl(epoll_fd,
                                  EPOLL_CTL_MOD,
                                  ready_fd,
                                  &read_event) < 0) {
                        clients.erase(client_it);
                    }
                }
            }
        }
    }
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
    if (!request->ParseFromString(rpc_request.body)) {
        return encodeErrorResponse("parse request body failed");
    }

    // 同步调用业务方法
    service->CallMethod(method,
                        nullptr,
                        request.get(),
                        response.get(),
                        nullptr);

    // 序列化 protobuf 响应对象
    std::string response_body;
    if (!response->SerializeToString(&response_body)) {
        return encodeErrorResponse("serialize response failed");
    }

    // 构造并编码成功响应
    RpcResponse rpc_response;
    rpc_response.body = std::move(response_body);
    return encodeRpcResponse(rpc_response);
}

} // namespace tinyrpc
