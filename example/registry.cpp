#include "tcp_socket.h"

#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

namespace {

std::unordered_map<std::string, std::pair<std::string, uint16_t>> services;
std::mutex services_mutex;

void handleClient(int client_fd) {
    std::string line;
    if (!tinyrpc::TcpSocket::recvLine(client_fd, line)) {
        close(client_fd);
        return;
    }

    std::istringstream iss(line);
    std::string command;
    iss >> command;

    std::string response = "ERROR\n";
    if (command == "REGISTER") {
        std::string service_name;
        std::string ip;
        uint16_t port = 0;

        if (iss >> service_name >> ip >> port) {
            std::lock_guard<std::mutex> lock(services_mutex);
            services[service_name] = {ip, port};
            response = "OK\n";
            std::cout << "register service: " << service_name << " " << ip << ":" << port << std::endl;
        }
    } else if (command == "DISCOVER") {
        std::string service_name;
        if (iss >> service_name) {
            std::lock_guard<std::mutex> lock(services_mutex);
            auto it = services.find(service_name);
            if (it == services.end()) {
                response = "NOT_FOUND\n";
            } else {
                response = "FOUND " + it->second.first + " " + std::to_string(it->second.second) + "\n";
            }
        }
    }

    tinyrpc::TcpSocket::sendAll(client_fd, response);
    close(client_fd);
}

} // namespace

int main() {
    int listen_fd = tinyrpc::TcpSocket::createServerSocket("127.0.0.1", 9000);
    if (listen_fd < 0) {
        std::cerr << "create registry socket failed" << std::endl;
        return 1;
    }

    std::cout << "tinyrpc registry start at 127.0.0.1:9000" << std::endl;

    while (true) {
        int client_fd = accept(listen_fd, nullptr, nullptr);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        std::thread(handleClient, client_fd).detach();
    }

    return 0;
}
