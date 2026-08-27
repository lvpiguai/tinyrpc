#pragma once

#include <tinyrpc/common/endpoint.h>

#include <string>
#include <vector>

namespace tinyrpc {

// 注册中心客户端，负责服务端点注册与发现
class RegistryClient {
public:
    // 配置注册中心地址
    explicit RegistryClient(Endpoint registry_endpoint);

    // 配置注册中心地址和请求超时
    RegistryClient(Endpoint registry_endpoint, int timeout_ms);

    // 注册服务端点
    bool registerServiceEndpoint(const std::string& service_name,
                                 const Endpoint& service_endpoint);

    // 续约服务端点
    bool heartbeatServiceEndpoint(const std::string& service_name,
                                  const Endpoint& service_endpoint);

    // 主动注销服务端点
    bool unregisterServiceEndpoint(const std::string& service_name,
                                   const Endpoint& service_endpoint);

    // 发现服务的全部实例
    std::vector<Endpoint> discoverServiceEndpoints(
        const std::string& service_name);

private:
    // 注册中心地址
    Endpoint registry_endpoint_;
    int timeout_ms_;
};

} // namespace tinyrpc
