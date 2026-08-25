#pragma once

#include <tinyrpc/common/endpoint.h>

#include <google/protobuf/service.h>

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>

namespace tinyrpc {

class TcpSocket;

// RPC 服务端
class RpcServer {
public:
    // 配置服务端监听地址
    explicit RpcServer(Endpoint listen_endpoint);

    // 注册本地业务服务
    void registerService(google::protobuf::Service& service);

    // 设置注册中心地址
    void setRegistry(Endpoint registry_endpoint);

    // 设置 RPC 请求处理线程池参数
    void setThreadPool(size_t worker_count, size_t max_queue_size);

    // 启动 RPC 服务并监听指定地址
    void run();

private:
    // 在线程池中处理完整请求帧并回写响应
    void handleClient(TcpSocket client_socket, std::string frame);

private:
    // 按服务全名保存业务实现，RpcServer 不拥有这些对象
    std::unordered_map<std::string, google::protobuf::Service*> services_;
    Endpoint listen_endpoint_;
    std::optional<Endpoint> registry_endpoint_;

    // 线程池配置
    size_t worker_count_ = 0;
    size_t max_queue_size_ = 1024;
};

} // namespace tinyrpc
