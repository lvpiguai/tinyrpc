#pragma once

#include <tinyrpc/common/endpoint.h>

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

    Mode mode_;
    Endpoint endpoint_;
};

} // namespace tinyrpc
