#pragma once

#include <tinyrpc/common/endpoint.h>

#include <optional>
#include <string>

namespace tinyrpc {

// 注册中心客户端，负责服务端点注册与发现
class RegistryClient {
public:
    // 配置注册中心地址
    explicit RegistryClient(Endpoint registry_endpoint);

    // 注册服务端点
    bool registerServiceEndpoint(const std::string& service_name,
                                 const Endpoint& service_endpoint);

    // 发现服务端点
    std::optional<Endpoint> discoverServiceEndpoint(
        const std::string& service_name);

private:
    // 注册中心地址
    Endpoint registry_endpoint_;
};

} // namespace tinyrpc
