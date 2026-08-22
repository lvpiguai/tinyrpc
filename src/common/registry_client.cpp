#include <tinyrpc/common/registry_client.h>

#include <tinyrpc/common/tcp_socket.h>

#include <sstream>
#include <string>

namespace tinyrpc {

RegistryClient::RegistryClient(const std::string& ip, uint16_t port)
    : registry_ip_(ip), registry_port_(port) {}

bool RegistryClient::registerService(const std::string& service_name,
                                     const std::string& ip,
                                     uint16_t port) {
    auto socket = TcpSocket::connect(registry_ip_, registry_port_);
    if (!socket) {
        return false;
    }

    std::ostringstream request;
    request << "REGISTER " << service_name << " " << ip << " " << port << "\n";

    std::string response;
    const auto ok = socket->sendAll(request.str()) &&
                    socket->recvLine(response) &&
                    response == "OK";

    return ok;
}

bool RegistryClient::discoverService(const std::string& service_name,
                                     std::string& ip,
                                     uint16_t& port) {
    auto socket = TcpSocket::connect(registry_ip_, registry_port_);
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
    uint16_t discovered_port = 0;
    if (!(iss >> status >> ip >> discovered_port) || status != "FOUND") {
        return false;
    }

    port = discovered_port;
    return true;
}

} // namespace tinyrpc
