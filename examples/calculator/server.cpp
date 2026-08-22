#include "calculator.pb.h"
#include <tinyrpc/server/rpc_provider.h>

#include <iostream>

class CalculatorServiceImpl : public tinyrpc::CalculatorService {
public:
    // Add 业务实现
    void Add(google::protobuf::RpcController* controller,
             const tinyrpc::AddRequest* request,
             tinyrpc::AddResponse* response,
             google::protobuf::Closure* done) override {
        // 取出参数
        const auto a = request->a();
        const auto b = request->b();

        // 执行计算
        const auto result = a + b;

        // 写入响应
        response->set_result(result);

        std::cout << "call Add: " << a << " + " << b << " = " << result << std::endl;

        // 通知框架回包
        if (done) {
            done->Run();
        }
    }
    // Sub 业务实现
    void Sub(google::protobuf::RpcController* controller,
             const tinyrpc::SubRequest* request,
             tinyrpc::SubResponse* response,
             google::protobuf::Closure* done) override {
        const auto a = request->a();
        const auto b = request->b();

        const auto result = a - b;
        response->set_result(result);

        std::cout << "call Sub: " << a << " - " << b << " = " << result << std::endl;

        if (done) {
            done->Run();
        }
    }
};

int main() {
    // 创建、配置 RpcProvider
    tinyrpc::RpcProvider provider;
    provider.setRegistry("127.0.0.1", 9000);//注册中心地址
    provider.setThreadPool(4, 1024);

    // 创建、注册、启动服务
    CalculatorServiceImpl calculator_service;
    provider.registerService(calculator_service);

    std::cout << "calculator server start at 127.0.0.1:8000" << std::endl;
    provider.run("127.0.0.1", 8000);//服务端地址

    return 0;
}
