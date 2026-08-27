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

    // 设置服务发现、建连及单次收发的超时时间
    void setTimeout(int timeout_ms);

    // 执行 Protobuf 生成代码转发的 RPC 调用
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    // 保存池中连接及其目标实例和占用状态
    struct PooledConnection {
        PooledConnection(TcpSocket socket_value,
                         Endpoint endpoint_value);

        TcpSocket socket;
        Endpoint endpoint;
        bool in_use = true;
    };

    using ConnectionPtr = std::shared_ptr<PooledConnection>;

    // 首次调用时绑定服务，后续调用验证服务是否一致
    std::optional<std::string> bindOrValidateService(
        const std::string& service_name);

    // 获取服务端点
    std::vector<Endpoint> getServiceEndpoints(int timeout_ms);

    // 创建 TCP 连接
    ConnectionPtr createConnection(int timeout_ms);

    // 获取一条空闲连接，连接池未满时创建新连接
    ConnectionPtr acquireConnection();

    // 归还可复用连接，或移除失效连接
    void releaseConnection(const ConnectionPtr& connection, bool reusable);

    Mode mode_;          // 直连或注册中心寻址
    Endpoint endpoint_;  // 服务端或注册中心地址

    // 首次调用后固定绑定的逻辑服务
    std::string service_name_;

    // 连接池状态，单条连接同一时间只允许一个 RPC 使用
    std::vector<ConnectionPtr> connections_;
    size_t max_connections_ = 4;
    int timeout_ms_ = TcpSocket::kDefaultTimeoutMs;
    size_t connecting_count_ = 0;  // 已预留但尚未建成的连接数
    size_t next_endpoint_index_ = 0;  // 下一次轮询的实例位置
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
};

} // namespace tinyrpc
