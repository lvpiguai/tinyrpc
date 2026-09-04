#include <tinyrpc/client/rpc_channel.h>

#include <tinyrpc/common/protocol_codec.h>
#include <tinyrpc/common/registry_client.h>
#include <tinyrpc/common/tcp_socket.h>
#include <tinyrpc/common/tcp_frame_transport.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

namespace tinyrpc {

// 配置直连或注册中心寻址方式
RpcChannel::RpcChannel(Mode mode, Endpoint endpoint)
    : mode_(mode), endpoint_(std::move(endpoint)) {}

// 创建一条已被当前调用占用的池化连接
RpcChannel::PooledConnection::PooledConnection(
    TcpSocket socket_value,
    Endpoint endpoint_value)
    : socket(std::move(socket_value)),
      endpoint(std::move(endpoint_value)) {}

// 首次调用时绑定服务，后续调用验证服务是否一致
std::optional<std::string> RpcChannel::bindOrValidateService(
    const std::string& service_name) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    if (service_name_.empty()) {
        service_name_ = service_name;
        return std::nullopt;
    }
    if (service_name_ != service_name) {
        return "rpc channel is already bound to service: " + service_name_;
    }
    return std::nullopt;
}

// 设置连接池容量并唤醒可能等待扩容的调用
void RpcChannel::setMaxConnections(size_t max_connections) {
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        max_connections_ = std::max<size_t>(1, max_connections);
    }
    pool_cv_.notify_all();
}

// 更新后续网络操作及已有连接的超时时间
void RpcChannel::setTimeout(int timeout_ms) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    timeout_ms_ = std::max(1, timeout_ms);

    // 配置更新时同步应用到连接池中的已有连接
    for (const auto& connection : connections_) {
        connection->socket.setTimeout(timeout_ms_);
    }
}

// 获取服务端点
std::vector<Endpoint> RpcChannel::getServiceEndpoints(int timeout_ms) {
    // 直连
    if (mode_ == Mode::Direct) {
        return {endpoint_};
    }

    // 服务发现
    RegistryClient registry(endpoint_, timeout_ms);
    auto endpoints = registry.discoverServiceEndpoints(service_name_);
    if (endpoints.empty()) {
        return {};
    }

    // 更新轮询位置
    size_t start_index = 0;
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        start_index = next_endpoint_index_ % endpoints.size();
        next_endpoint_index_ = (next_endpoint_index_ + 1) % endpoints.size();
    }

    // 调整尝试顺序
    std::rotate(endpoints.begin(),
                endpoints.begin() + start_index,
                endpoints.end());
    return endpoints;
}

// 创建 TCP 连接
RpcChannel::ConnectionPtr RpcChannel::createConnection(int timeout_ms) {
    const auto endpoints = getServiceEndpoints(timeout_ms);

    // 依次尝试建连
    for (const auto& endpoint : endpoints) {
        auto socket = TcpSocket::connect(endpoint, timeout_ms);
        if (socket) {
            return std::make_shared<PooledConnection>(
                std::move(*socket), endpoint);
        }
    }
    return nullptr;
}

// 获取空闲连接；达到上限时等待其他调用归还连接
RpcChannel::ConnectionPtr RpcChannel::acquireConnection() {
    // 锁定连接池
    std::unique_lock<std::mutex> lock(pool_mutex_);

    // 唤醒后重新检查
    while (true) {
        // 复用空闲连接
        const auto available = std::find_if(
            connections_.begin(), connections_.end(),
            [](const ConnectionPtr& connection) {
                return !connection->in_use;
            });
        if (available != connections_.end()) {
            (*available)->in_use = true;
            return *available;
        }

        // 池满则等待
        if (connections_.size() + connecting_count_ >= max_connections_) {
            pool_cv_.wait(lock);
            continue;
        }

        // 预留建连名额
        const auto timeout_ms = timeout_ms_;
        ++connecting_count_;
        lock.unlock();

        // 创建连接
        auto connection = createConnection(timeout_ms);

        // 释放建连名额
        lock.lock();
        --connecting_count_;

        // 处理建连失败
        if (!connection) {
            lock.unlock();
            pool_cv_.notify_one();
            return nullptr;
        }

        // 加入连接池
        connections_.push_back(connection);
        lock.unlock();
        return connection;
    }
}

// 归还或移除连接
void RpcChannel::releaseConnection(const ConnectionPtr& connection,
                                   bool reusable) {
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);

        // 查找连接
        const auto item = std::find(connections_.begin(),
                                    connections_.end(),
                                    connection);
        if (item == connections_.end()) {
            return;
        }

        // 清除失效实例的空闲连接
        if (!reusable) {
            connections_.erase(
                std::remove_if(
                    connections_.begin(), connections_.end(),
                    [&](const ConnectionPtr& candidate) {
                        return candidate == connection ||
                               (!candidate->in_use &&
                                candidate->endpoint == connection->endpoint);
                    }),
                connections_.end());
        } else if (connections_.size() > max_connections_) {
            // 收缩超额连接
            connections_.erase(item);
        } else {
            // 标记为空闲
            connection->in_use = false;
        }
    }
    pool_cv_.notify_one();
}

// 执行一次同步 RPC 调用
void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
    // 统一记录错误并通知调用方
    auto fail = [&](const std::string& message) {
        if (controller) {
            controller->SetFailed(message);
        }
        if (done) {
            done->Run();
        }
    };

    // 绑定或校验当前 Channel 的逻辑服务
    const auto& service_name = method->service()->full_name();
    if (auto error = bindOrValidateService(service_name)) {
        fail(*error);
        return;
    }

    // 序列化请求体
    std::string serialized_request_body;
    if (!request->SerializeToString(&serialized_request_body)) {
        fail("serialize request failed");
        return;
    }

    // 构造 RPC 请求对象
    RpcRequest rpc_request{
        service_name,
        method->name(),
        std::move(serialized_request_body)
    };

    // 编码 RPC 请求帧
    std::string frame;
    auto status = protocol_codec::encode(rpc_request, frame);
    if (!status.ok()) {
        fail(status.message);
        return;
    }

    // 从连接池独占一条连接
    auto connection = acquireConnection();
    if (!connection) {
        fail("connect or discover service failed");
        return;
    }

    TcpFrameTransport transport(connection->socket);

    // 发送 RPC 请求帧
    status = transport.sendFrame(frame);
    if (!status.ok()) {
        releaseConnection(connection, false);
        fail(status.message);
        return;
    }

    // 接收 RPC 响应帧
    status = transport.receiveFrame(frame);
    if (!status.ok()) {
        releaseConnection(connection, false);
        fail(status.message);
        return;
    }

    // 解码 RPC 响应帧
    RpcResponse rpc_response;
    status = protocol_codec::decode(frame, rpc_response);
    if (!status.ok()) {
        releaseConnection(connection, false);
        fail(status.message);
        return;
    }

    // 检查服务端处理结果
    if (!rpc_response.success) {
        auto err = rpc_response.error_message;
        if (err.empty()) {
            err = "rpc server error";
        }
        releaseConnection(connection, true);
        fail(err);
        return;
    }

    // 解析 protobuf 响应对象
    if (!response->ParseFromString(rpc_response.serialized_body)) {
        releaseConnection(connection, true);
        fail("parse response failed");
        return;
    }

    // 请求和响应完整结束后，将健康连接归还连接池
    releaseConnection(connection, true);

    // 通知调用方完成
    if (done) {
        done->Run();
    }
}

} // namespace tinyrpc
