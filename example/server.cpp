#include "calculator.pb.h"
#include "rpc_provider.h"

#include <iostream>

class CalculatorServiceImpl : public tinyrpc::CalculatorService {
public:
    // Add 业务实现
    void Add(google::protobuf::RpcController* controller,
             const tinyrpc::AddRequest* request,
             tinyrpc::AddResponse* response,
             google::protobuf::Closure* done) override {
        // 取出参数
        int a = request->a();
        int b = request->b();

        // 执行计算
        int result = a + b;

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
        int a = request->a();
        int b = request->b();

        int result = a - b;
        response->set_result(result);

        std::cout << "call Sub: " << a << " - " << b << " = " << result << std::endl;

        if (done) {
            done->Run();
        }
    }
};

int main() {
    // 创建服务对象
    CalculatorServiceImpl calculator_service;

    // 注册并启动服务
    tinyrpc::RpcProvider provider;
    provider.registerService(&calculator_service);

    provider.run("127.0.0.1", 8000);

    return 0;
}
