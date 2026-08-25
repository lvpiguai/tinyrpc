#pragma once

#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/common/tcp_socket.h>

#include <google/protobuf/service.h>

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace tinyrpc {

// 客户端 RPC 通道
class RpcChannel : public google::protobuf::RpcChannel {
public:
    // 目标服务寻址方式
    enum class Mode {
        Direct,
        Registry
    };

    // 配置寻址方式和网络端点
    RpcChannel(Mode mode, Endpoint endpoint);

    // 设置连接池最大连接数，最小值为 1
    void setMaxConnections(size_t max_connections);

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    struct PooledConnection {
        PooledConnection(TcpSocket socket_value,
                         std::string service_name_value);

        TcpSocket socket;
        std::string service_name;
        bool in_use = true;
    };

    using ConnectionPtr = std::shared_ptr<PooledConnection>;

    std::optional<Endpoint> findServiceEndpoint(
        const std::string& service_name);

    // 获取一条空闲连接，连接池未满时创建新连接
    ConnectionPtr acquireConnection(const std::string& service_name);

    // 归还健康连接，或移除失效连接
    void releaseConnection(const ConnectionPtr& connection, bool healthy);

    Mode mode_;
    Endpoint endpoint_;

    // 连接池状态，单条连接同一时间只允许一个 RPC 使用
    std::vector<ConnectionPtr> connections_;
    size_t max_connections_ = 4;
    size_t connecting_count_ = 0;
    std::unordered_map<std::string, size_t> next_endpoint_indices_;
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
};

} // namespace tinyrpc
