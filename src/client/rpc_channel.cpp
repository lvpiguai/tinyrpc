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

RpcChannel::RpcChannel(Mode mode, Endpoint endpoint)
    : mode_(mode), endpoint_(std::move(endpoint)) {}

RpcChannel::PooledConnection::PooledConnection(
    TcpSocket socket_value,
    std::string service_name_value,
    Endpoint endpoint_value)
    : socket(std::move(socket_value)),
      service_name(std::move(service_name_value)),
      endpoint(std::move(endpoint_value)) {}

void RpcChannel::setMaxConnections(size_t max_connections) {
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        max_connections_ = std::max<size_t>(1, max_connections);
    }
    pool_cv_.notify_all();
}

void RpcChannel::setTimeout(int timeout_ms) {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    timeout_ms_ = std::max(1, timeout_ms);

    // 配置更新时同步应用到连接池中的已有连接
    for (const auto& connection : connections_) {
        connection->socket.setTimeout(timeout_ms_);
    }
}

// 返回本次建连的候选端点，注册模式从轮询位置开始排列
std::vector<Endpoint> RpcChannel::findServiceEndpoints(
    const std::string& service_name,
    int timeout_ms) {
    // 使用直连服务端地址
    if (mode_ == Mode::Direct) {
        return {endpoint_};
    }

    // 获取全部服务实例，由客户端选择本次连接的目标
    RegistryClient registry(endpoint_, timeout_ms);
    const auto endpoints = registry.discoverServiceEndpoints(service_name);
    if (endpoints.empty()) {
        return {};
    }

    // 每个服务独立维护轮询起点，并保留其余实例作为建连备选
    size_t start_index = 0;
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        auto& next_index = next_endpoint_indices_[service_name];
        start_index = next_index % endpoints.size();
        next_index = (next_index + 1) % endpoints.size();
    }

    std::vector<Endpoint> ordered_endpoints;
    ordered_endpoints.reserve(endpoints.size());
    for (size_t i = 0; i < endpoints.size(); ++i) {
        ordered_endpoints.push_back(
            endpoints[(start_index + i) % endpoints.size()]);
    }
    return ordered_endpoints;
}

// 获取空闲连接；达到上限时等待其他调用归还连接
RpcChannel::ConnectionPtr RpcChannel::acquireConnection(
    const std::string& service_name) {
    std::unique_lock<std::mutex> lock(pool_mutex_);

    while (true) {
        // 直连模式下所有服务共用目标地址；注册模式要求服务名相同
        const auto available = std::find_if(
            connections_.begin(), connections_.end(),
            [&](const ConnectionPtr& connection) {
                const auto matches_service =
                    mode_ == Mode::Direct ||
                    connection->service_name == service_name;
                return matches_service && !connection->in_use;
            });
        if (available != connections_.end()) {
            (*available)->in_use = true;
            return *available;
        }

        // 注册模式下连接池已满时，可淘汰其他服务的空闲连接
        if (connections_.size() + connecting_count_ >= max_connections_) {
            const auto replaceable = std::find_if(
                connections_.begin(), connections_.end(),
                [&](const ConnectionPtr& connection) {
                    return !connection->in_use &&
                           mode_ == Mode::Registry &&
                           connection->service_name != service_name;
                });
            if (replaceable != connections_.end()) {
                connections_.erase(replaceable);
                continue;
            }

            pool_cv_.wait(lock);
            continue;
        }

        // 先预留连接名额，再释放锁执行可能阻塞的服务发现和 connect
        const auto timeout_ms = timeout_ms_;
        ++connecting_count_;
        lock.unlock();

        const auto target_endpoints = findServiceEndpoints(service_name,
                                                           timeout_ms);
        std::optional<TcpSocket> socket;
        std::optional<Endpoint> connected_endpoint;
        // 只在发送请求前尝试其他实例，不重放已经发送的 RPC
        for (const auto& target_endpoint : target_endpoints) {
            socket = TcpSocket::connect(target_endpoint.ip,
                                        target_endpoint.port,
                                        timeout_ms);
            if (socket) {
                connected_endpoint = target_endpoint;
                break;
            }
        }

        lock.lock();
        --connecting_count_;

        if (!socket || !connected_endpoint) {
            lock.unlock();
            pool_cv_.notify_all();
            return nullptr;
        }

        auto connection = std::make_shared<PooledConnection>(
            std::move(*socket), service_name, std::move(*connected_endpoint));
        connections_.push_back(connection);
        lock.unlock();
        pool_cv_.notify_all();
        return connection;
    }
}

void RpcChannel::releaseConnection(const ConnectionPtr& connection,
                                   bool healthy) {
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        const auto item = std::find(connections_.begin(),
                                    connections_.end(),
                                    connection);
        if (item == connections_.end()) {
            return;
        }

        // 网络异常时，同时清除同一实例的其他空闲旧连接
        if (!healthy) {
            const auto failed_endpoint = connection->endpoint;
            connections_.erase(
                std::remove_if(
                    connections_.begin(), connections_.end(),
                    [&](const ConnectionPtr& candidate) {
                        const auto same_endpoint =
                            candidate->endpoint.ip == failed_endpoint.ip &&
                            candidate->endpoint.port == failed_endpoint.port;
                        return candidate == connection ||
                               (!candidate->in_use && same_endpoint);
                    }),
                connections_.end());
        } else if (connections_.size() > max_connections_) {
            connections_.erase(item);
        } else {
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

    // 序列化请求体
    std::string request_body;
    if (!request->SerializeToString(&request_body)) {
        fail("serialize request failed");
        return;
    }

    // 构造 RPC 请求对象
    RpcRequest rpc_request{
        method->service()->full_name(),
        method->name(),
        std::move(request_body)
    };

    // 编码 RPC 请求帧
    std::string frame;
    auto status = protocol_codec::encode(rpc_request, frame);
    if (!status.ok()) {
        fail(status.message);
        return;
    }

    // 从连接池独占一条连接
    auto connection = acquireConnection(rpc_request.service_name);
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
    if (!response->ParseFromString(rpc_response.body)) {
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
