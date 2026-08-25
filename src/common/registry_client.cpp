#include <tinyrpc/common/registry_client.h>

#include <tinyrpc/common/tcp_socket.h>

#include <cstddef>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace tinyrpc {

namespace {

constexpr std::size_t kMaxRegistryLineSize = 64 * 1024;

} // namespace

// 配置注册中心地址
RegistryClient::RegistryClient(Endpoint registry_endpoint)
    : RegistryClient(std::move(registry_endpoint),
                     TcpSocket::kDefaultTimeoutMs) {}

RegistryClient::RegistryClient(Endpoint registry_endpoint, int timeout_ms)
    : registry_endpoint_(std::move(registry_endpoint)),
      timeout_ms_(timeout_ms) {}

// 向注册中心注册服务端点
bool RegistryClient::registerServiceEndpoint(
    const std::string& service_name,
    const Endpoint& service_endpoint) {
    // 连接注册中心
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port,
                                     timeout_ms_);
    if (!socket) {
        return false;
    }

    // 构造并发送注册请求
    std::ostringstream request;
    request << "REGISTER " << service_name << " "
            << service_endpoint.ip << " " << service_endpoint.port << "\n";

    if (!socket->sendAll(request.str())) {
        return false;
    }

    const auto response = socket->recvLine(kMaxRegistryLineSize);
    return response && *response == "OK";
}

// 向注册中心发送服务心跳
bool RegistryClient::heartbeatServiceEndpoint(
    const std::string& service_name,
    const Endpoint& service_endpoint) {
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port,
                                     timeout_ms_);
    if (!socket) {
        return false;
    }

    std::ostringstream request;
    request << "HEARTBEAT " << service_name << " "
            << service_endpoint.ip << " " << service_endpoint.port << "\n";
    if (!socket->sendAll(request.str())) {
        return false;
    }

    const auto response = socket->recvLine(kMaxRegistryLineSize);
    return response && *response == "OK";
}

// 从注册中心主动注销服务端点
bool RegistryClient::unregisterServiceEndpoint(
    const std::string& service_name,
    const Endpoint& service_endpoint) {
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port,
                                     timeout_ms_);
    if (!socket) {
        return false;
    }

    std::ostringstream request;
    request << "UNREGISTER " << service_name << " "
            << service_endpoint.ip << " " << service_endpoint.port << "\n";
    if (!socket->sendAll(request.str())) {
        return false;
    }

    const auto response = socket->recvLine(kMaxRegistryLineSize);
    return response && *response == "OK";
}

// 从注册中心发现服务的全部实例
std::vector<Endpoint> RegistryClient::discoverServiceEndpoints(
    const std::string& service_name) {
    // 连接注册中心
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port,
                                     timeout_ms_);
    if (!socket) {
        return {};
    }

    // 发送服务发现请求
    const auto request = "DISCOVER " + service_name + "\n";
    if (!socket->sendAll(request)) {
        return {};
    }

    const auto response = socket->recvLine(kMaxRegistryLineSize);
    if (!response) {
        return {};
    }

    // 解析：FOUND <数量> <ip1> <port1> ...
    std::istringstream iss(*response);
    std::string status;
    std::size_t endpoint_count = 0;
    if (!(iss >> status >> endpoint_count) || status != "FOUND") {
        return {};
    }

    std::vector<Endpoint> endpoints;
    endpoints.reserve(endpoint_count);
    for (std::size_t i = 0; i < endpoint_count; ++i) {
        std::string ip;
        unsigned int port = 0;
        if (!(iss >> ip >> port) || port > UINT16_MAX) {
            return {};
        }
        endpoints.push_back(
            Endpoint{std::move(ip), static_cast<uint16_t>(port)});
    }

    return endpoints;
}

} // namespace tinyrpc
