#pragma once

#include <tinyrpc/common/endpoint.h>

#include <string>

namespace tinyrpc {

class RegistryClient {
public:
    explicit RegistryClient(Endpoint registry_endpoint);

    bool registerService(const std::string& service_name,
                         const Endpoint& service_endpoint);

    bool discoverService(const std::string& service_name,
                         Endpoint& service_endpoint);

private:
    Endpoint registry_endpoint_;
};

} // namespace tinyrpc
