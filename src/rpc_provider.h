#pragma once

#include <google/protobuf/service.h>

#include <cstdint>
#include <string>
#include <unordered_map>

namespace tinyrpc {

// 服务端 RPC 分发器
class RpcProvider {
public:
    void registerService(google::protobuf::Service* service);

    void run(const std::string& ip, uint16_t port);

private:
    // service 及方法索引
    struct ServiceInfo {
        google::protobuf::Service* service = nullptr;
        std::unordered_map<std::string, const google::protobuf::MethodDescriptor*> methods;
    };

    void handleClient(int client_fd);

private:
    std::unordered_map<std::string, ServiceInfo> services_;
};

} // namespace tinyrpc
