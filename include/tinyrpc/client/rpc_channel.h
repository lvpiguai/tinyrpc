#pragma once

#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/common/tcp_socket.h>

#include <google/protobuf/service.h>

#include <optional>
#include <string>

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

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    std::optional<Endpoint> findServiceEndpoint(
        const std::string& service_name) const;

    // 获取当前长连接，不存在时连接目标服务
    TcpSocket* getConnection(const std::string& service_name);

    // 关闭失效连接并清理关联服务名
    void closeConnection();

    Mode mode_;
    Endpoint endpoint_;

    // 同一个 RpcChannel 的串行调用复用该连接
    std::optional<TcpSocket> socket_;
    std::string connected_service_name_;
};

} // namespace tinyrpc
