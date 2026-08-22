#include <tinyrpc/common/tcp_socket.h>

#include <cstdint>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

namespace {

std::unordered_map<std::string, std::pair<std::string, uint16_t>> services;
std::mutex services_mutex;

void handleClient(tinyrpc::TcpSocket client_socket) {
    std::string line;
    if (!client_socket.recvLine(line)) {
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
            const auto it = services.find(service_name);
            if (it == services.end()) {
                response = "NOT_FOUND\n";
            } else {
                response = "FOUND " + it->second.first + " " + std::to_string(it->second.second) + "\n";
            }
        }
    }

    client_socket.sendAll(response);
}

} // namespace

int main() {
    auto listener = tinyrpc::TcpListener::bind("127.0.0.1", 9000);
    if (!listener) {
        std::cerr << "create registry socket failed" << std::endl;
        return 1;
    }

    std::cout << "tinyrpc registry start at 127.0.0.1:9000" << std::endl;

    while (true) {
        auto client_socket = listener->accept();
        if (!client_socket) {
            std::cerr << "accept client failed" << std::endl;
            continue;
        }

        std::thread(handleClient, std::move(*client_socket)).detach();
    }

    return 0;
}
