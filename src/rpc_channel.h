#pragma once

#include <google/protobuf/service.h>

#include <cstdint>
#include <string>

namespace tinyrpc {

// 客户端 RPC 通道
class RpcChannel : public google::protobuf::RpcChannel {
public:
    RpcChannel(const std::string& ip, uint16_t port);

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    std::string ip_;
    uint16_t port_;
};

} // namespace tinyrpc
