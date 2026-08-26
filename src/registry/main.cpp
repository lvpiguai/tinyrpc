#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/registry/registry_server.h>

#include <arpa/inet.h>

#include <charconv>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>

namespace {

constexpr std::string_view kDefaultIp = "127.0.0.1";
constexpr uint16_t kDefaultPort = 9000;

void printUsage(const char* program) {
    std::cerr << "Usage: " << program << " [ip port]" << std::endl;
}

bool parsePort(std::string_view text, uint16_t& port) {
    unsigned int value = 0;
    const auto [ptr, error] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || ptr != text.data() + text.size() ||
        value == 0 || value > UINT16_MAX) {
        return false;
    }

    port = static_cast<uint16_t>(value);
    return true;
}

bool isValidIpv4(const std::string& ip) {
    in_addr address{};
    return inet_pton(AF_INET, ip.c_str(), &address) == 1;
}

}  // namespace

int main(int argc, char* argv[]) {
    std::string ip{kDefaultIp};
    uint16_t port = kDefaultPort;

    if (argc != 1 && argc != 3) {
        printUsage(argv[0]);
        return 1;
    }

    if (argc == 3) {
        ip = argv[1];
        if (!isValidIpv4(ip)) {
            std::cerr << "Invalid IPv4 address: " << ip << std::endl;
            printUsage(argv[0]);
            return 1;
        }
        if (!parsePort(argv[2], port)) {
            std::cerr << "Invalid port: " << argv[2]
                      << " (expected 1-65535)" << std::endl;
            printUsage(argv[0]);
            return 1;
        }
    }

    // 创建并启动注册中心
    tinyrpc::RegistryServer server(tinyrpc::Endpoint{std::move(ip), port});
    server.run();

    return 0;
}
