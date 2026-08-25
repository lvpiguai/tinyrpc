#include <tinyrpc/common/endpoint.h>
#include <tinyrpc/registry/registry_server.h>

int main() {
    // 创建并启动注册中心
    tinyrpc::RegistryServer server(
        tinyrpc::Endpoint{"127.0.0.1", 9000});
    server.run();

    return 0;
}
