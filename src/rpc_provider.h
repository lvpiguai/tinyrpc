#pragma once

#include <google/protobuf/service.h>

#include <cstdint>
#include <cstddef>
#include <string>
#include <unordered_map>

namespace tinyrpc {

// 服务端 RPC 分发器
class RpcProvider {
public:
    void registerService(google::protobuf::Service* service);

    void setRegistry(const std::string& ip, uint16_t port);

    // 设置 RPC 请求处理线程池参数
    void setThreadPool(size_t worker_count, size_t max_queue_size);

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
    bool use_registry_ = false;
    std::string registry_ip_;
    uint16_t registry_port_ = 0;

    // 线程池配置
    size_t worker_count_ = 0;
    size_t max_queue_size_ = 1024;
};

} // namespace tinyrpc
