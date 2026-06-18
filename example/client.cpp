#include "calculator.pb.h"
#include "rpc_channel.h"
#include "rpc_controller.h"

#include <iostream>

int main() {
    // 创建 RPC 通道
    tinyrpc::RpcChannel channel;
    channel.setRegistry("127.0.0.1", 9000);

    // 创建客户端 stub
    tinyrpc::CalculatorService_Stub stub(&channel);

    // Add 业务测试
    {
        // 构造请求
        tinyrpc::AddRequest request;
        request.set_a(2);
        request.set_b(3);

        tinyrpc::AddResponse response;
        tinyrpc::RpcController controller;

        // 发起远程调用
        stub.Add(&controller, &request, &response, nullptr);
        if (controller.Failed()) {
            std::cerr << "RPC failed: " << controller.ErrorText() << std::endl;
        } else {
            std::cout << "RPC result: " << response.result() << std::endl;
        }
    }   
    // Sub 业务测试
    {
        tinyrpc::SubRequest request;
        request.set_a(10);
        request.set_b(4);

        tinyrpc::SubResponse response;
        tinyrpc::RpcController controller;

        stub.Sub(&controller, &request, &response, nullptr);
        if (controller.Failed()) {
            std::cerr << "RPC failed: " << controller.ErrorText() << std::endl;
        } else {
            std::cout << "Sub result: " << response.result() << std::endl;
        }
    }

    return 0;
}
