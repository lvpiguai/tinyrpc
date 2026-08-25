#pragma once

#include <tinyrpc/common/endpoint.h>

#include <mutex>
#include <string>
#include <unordered_map>

namespace tinyrpc {

class TcpSocket;

// 注册中心服务端，负责服务端点注册与发现
class RegistryServer {
public:
    // 配置注册中心监听地址
    explicit RegistryServer(Endpoint listen_endpoint);

    // 启动注册中心
    void run();

private:
    // 处理单个注册中心连接
    void handleClient(TcpSocket client_socket);

    // 注册中心监听地址
    Endpoint listen_endpoint_;

    // 保存服务名到服务端点的映射
    std::unordered_map<std::string, Endpoint> service_endpoints_;

    // 保护服务端点映射
    std::mutex mutex_;
};

} // namespace tinyrpc
