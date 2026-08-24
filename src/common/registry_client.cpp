#include <tinyrpc/common/registry_client.h>

#include <tinyrpc/common/tcp_socket.h>

#include <sstream>
#include <string>
#include <utility>

namespace tinyrpc {

RegistryClient::RegistryClient(Endpoint registry_endpoint)
    : registry_endpoint_(std::move(registry_endpoint)) {}

bool RegistryClient::registerService(const std::string& service_name,
                                     const Endpoint& service_endpoint) {
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port);
    if (!socket) {
        return false;
    }

    std::ostringstream request;
    request << "REGISTER " << service_name << " "
            << service_endpoint.ip << " " << service_endpoint.port << "\n";

    std::string response;
    const auto ok = socket->sendAll(request.str()) &&
                    socket->recvLine(response) &&
                    response == "OK";

    return ok;
}

bool RegistryClient::discoverService(const std::string& service_name,
                                     Endpoint& service_endpoint) {
    auto socket = TcpSocket::connect(registry_endpoint_.ip,
                                     registry_endpoint_.port);
    if (!socket) {
        return false;
    }

    const auto request = "DISCOVER " + service_name + "\n";
    std::string response;
    const auto ok = socket->sendAll(request) &&
                    socket->recvLine(response);

    if (!ok) {
        return false;
    }

    std::istringstream iss(response);
    std::string status;
    std::string discovered_ip;
    uint16_t discovered_port = 0;
    if (!(iss >> status >> discovered_ip >> discovered_port) ||
        status != "FOUND") {
        return false;
    }

    service_endpoint = Endpoint{std::move(discovered_ip), discovered_port};
    return true;
}

} // namespace tinyrpc
