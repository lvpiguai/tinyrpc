#pragma once

#include <cstdint>
#include <string>

namespace tinyrpc {

class RegistryClient {
public:
    RegistryClient(const std::string& ip, uint16_t port);

    bool registerService(const std::string& service_name,
                         const std::string& ip,
                         uint16_t port);

    bool discoverService(const std::string& service_name,
                         std::string& ip,
                         uint16_t& port);

private:
    std::string registry_ip_;
    uint16_t registry_port_;
};

} // namespace tinyrpc
