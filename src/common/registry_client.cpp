#include <tinyrpc/common/registry_client.h>

#include <tinyrpc/common/tcp_socket.h>

#include <cstddef>
#include <sstream>
#include <string>
#include <utility>

namespace tinyrpc {

namespace {

constexpr std::size_t kMaxRegistryLineSize = 1024;

} // namespace

// 配置注册中心地址
RegistryClient::RegistryClient(Endpoint registry_endpoint)
    : registry_endpoint_(std::move(registry_endpoint)) {}

// 向注册中心注册服务端点
bool RegistryClient::registerServiceEndpoint(
    const std::string& service_name,
    const Endpoint& service_endpoint) {
    // 连接注册中心
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port);
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

// 从注册中心发现服务端点
std::optional<Endpoint> RegistryClient::discoverServiceEndpoint(
    const std::string& service_name) {
    // 连接注册中心
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port);
    if (!socket) {
        return std::nullopt;
    }

    // 发送服务发现请求
    const auto request = "DISCOVER " + service_name + "\n";
    if (!socket->sendAll(request)) {
        return std::nullopt;
    }

    const auto response = socket->recvLine(kMaxRegistryLineSize);
    if (!response) {
        return std::nullopt;
    }

    // 解析服务端点
    std::istringstream iss(*response);
    std::string status;
    std::string discovered_ip;
    uint16_t discovered_port = 0;
    if (!(iss >> status >> discovered_ip >> discovered_port) ||
        status != "FOUND") {
        return std::nullopt;
    }

    return Endpoint{std::move(discovered_ip), discovered_port};
}

} // namespace tinyrpc
