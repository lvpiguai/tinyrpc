#pragma once

#include <google/protobuf/service.h>

#include <cstdint>
#include <cstddef>
#include <string>
#include <unordered_map>

namespace tinyrpc {

class TcpSocket;

// 服务端 RPC 分发器
class RpcProvider {
public:
    // 注册本地业务服务
    void registerService(google::protobuf::Service& service);

    // 设置注册中心地址
    void setRegistry(const std::string& ip, uint16_t port);

    // 设置 RPC 请求处理线程池参数
    void setThreadPool(size_t worker_count, size_t max_queue_size);

    // 启动 RPC 服务并监听指定地址
    void run(const std::string& ip, uint16_t port);

private:
    // 处理单个客户端连接
    void handleClient(TcpSocket client_socket);

private:
    // 按服务全名保存业务实现，Provider 不拥有这些对象
    std::unordered_map<std::string, google::protobuf::Service*> services_;
    bool use_registry_ = false;
    std::string registry_ip_;
    uint16_t registry_port_ = 0;

    // 线程池配置
    size_t worker_count_ = 0;
    size_t max_queue_size_ = 1024;
};

} // namespace tinyrpc
