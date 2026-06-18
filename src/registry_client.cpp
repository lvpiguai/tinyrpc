#include "registry_client.h"

#include "tcp_socket.h"

#include <unistd.h>

#include <sstream>
#include <string>

namespace tinyrpc {

RegistryClient::RegistryClient(const std::string& ip, uint16_t port)
    : registry_ip_(ip), registry_port_(port) {}

bool RegistryClient::registerService(const std::string& service_name,
                                     const std::string& ip,
                                     uint16_t port) {
    int fd = TcpSocket::connectToServer(registry_ip_, registry_port_);
    if (fd < 0) {
        return false;
    }

    std::ostringstream request;
    request << "REGISTER " << service_name << " " << ip << " " << port << "\n";

    std::string response;
    bool ok = TcpSocket::sendAll(fd, request.str()) &&
              TcpSocket::recvLine(fd, response) &&
              response == "OK";

    close(fd);
    return ok;
}

bool RegistryClient::discoverService(const std::string& service_name,
                                     std::string& ip,
                                     uint16_t& port) {
    int fd = TcpSocket::connectToServer(registry_ip_, registry_port_);
    if (fd < 0) {
        return false;
    }

    std::string request = "DISCOVER " + service_name + "\n";
    std::string response;
    bool ok = TcpSocket::sendAll(fd, request) &&
              TcpSocket::recvLine(fd, response);

    close(fd);
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
