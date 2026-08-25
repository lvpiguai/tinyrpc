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
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
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

// 保存尚未形成完整请求帧的连接及其读取状态
struct ClientConnection {
    explicit ClientConnection(TcpSocket value)
        : socket(std::move(value)) {}

    TcpSocket socket;                         // 连接所有权
    std::string input;                        // 已读取但尚未处理的数据
    std::optional<std::size_t> frame_size;    // 长度字段解析后的帧大小
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

// 编码并发送 RPC 响应帧
bool sendRpcResponse(TcpFrameTransport& transport, const RpcResponse& response) {
    std::string frame;
    const auto status = protocol_codec::encode(response, frame);
    return status.ok() && transport.sendFrame(frame).ok();
}

// 发送成功响应
bool sendSuccessResponse(TcpFrameTransport& transport,
                         const std::string& response_body) {
    RpcResponse response;
    response.body = response_body;
    return sendRpcResponse(transport, response);
}

// 发送错误响应
bool sendErrorResponse(TcpFrameTransport& transport,
                       const std::string& error_message) {
    RpcResponse response;
    response.success = false;
    response.error_message = error_message;
    return sendRpcResponse(transport, response);
}

} // namespace

// 配置服务端监听地址
RpcServer::RpcServer(Endpoint listen_endpoint)
    : listen_endpoint_(std::move(listen_endpoint)) {}

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

    // 创建 RPC 请求处理线程池
    ThreadPool thread_pool(worker_count, max_queue_size_);

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

    // Reactor 线程持有尚未交给工作线程的客户端连接
    std::unordered_map<int, ClientConnection> clients;
    std::array<epoll_event, kMaxEpollEvents> events{};

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
            close(epoll_fd);
            return;
        }

        for (int i = 0; i < event_count; ++i) {
            // epoll 通过 data.fd 告知本次就绪的文件描述符
            const auto ready_fd = events[i].data.fd;
            const auto ready_events = events[i].events;

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

            if ((ready_events & EPOLLIN) == 0) {
                continue;
            }

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

            // 完整帧到达后移出 epoll，再交给工作线程
            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, ready_fd, nullptr);

            // 当前响应仍使用阻塞 sendAll，移交前恢复阻塞模式
            if (!client_it->second.socket.setNonBlocking(false)) {
                clients.erase(client_it);
                continue;
            }
            auto shared_socket = std::make_shared<TcpSocket>(
                std::move(client_it->second.socket));
            clients.erase(client_it);

            // shared_ptr 让包含移动型 Socket 的任务可以存入 std::function
            const auto submitted = thread_pool.submit(
                [this, shared_socket, frame = std::move(frame)]() mutable {
                    handleClient(std::move(*shared_socket), std::move(frame));
                });
            if (!submitted) {
                std::cerr << "rpc thread pool queue is full, reject connection"
                          << std::endl;
            }
        }
    }
}

// 处理单个客户端连接
void RpcServer::handleClient(TcpSocket client_socket, std::string frame) {
    TcpFrameTransport transport(client_socket);

    // 解码 RPC 请求帧
    RpcRequest rpc_request;
    auto status = protocol_codec::decode(frame, rpc_request);
    if (!status.ok()) {
        return;
    }

    // 读取目标服务和方法
    const auto& service_name = rpc_request.service_name;
    const auto& method_name = rpc_request.method_name;

    // 查找目标服务
    const auto service_it = services_.find(service_name);
    if (service_it == services_.end()) {
        const auto err = "service not found: " + service_name;
        sendErrorResponse(transport, err);
        return;
    }

    // 查找目标方法
    auto* service = service_it->second;
    const auto* method =
        service->GetDescriptor()->FindMethodByName(method_name);
    if (method == nullptr) {
        const auto err = "method not found: " + method_name;
        sendErrorResponse(transport, err);
        return;
    }

    // 创建 protobuf 请求和响应对象
    std::unique_ptr<google::protobuf::Message> request(
        service->GetRequestPrototype(method).New());
    std::unique_ptr<google::protobuf::Message> response(
        service->GetResponsePrototype(method).New());

    // 解析 protobuf 请求对象
    if (!request->ParseFromString(rpc_request.body)) {
        std::string err = "parse request body failed";
        sendErrorResponse(transport, err);
        return;
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
        sendErrorResponse(transport, "serialize response failed");
        return;
    }

    // 回写 RPC 响应消息
    sendSuccessResponse(transport, response_body);
}

} // namespace tinyrpc
