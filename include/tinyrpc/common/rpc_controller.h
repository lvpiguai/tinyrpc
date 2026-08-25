#pragma once

#include <google/protobuf/service.h>

#include <string>

namespace tinyrpc {

// RPC 调用控制器，保存调用失败状态
class RpcController : public google::protobuf::RpcController {
public:
    // 初始化为未失败状态
    RpcController();

    // 重置调用状态
    void Reset() override;

    // 判断调用是否失败
    bool Failed() const override;

    // 获取调用错误信息
    std::string ErrorText() const override;

    // 请求取消调用
    void StartCancel() override;

    // 标记调用失败
    void SetFailed(const std::string& reason) override;

    // 判断调用是否已取消
    bool IsCanceled() const override;

    // 注册取消回调
    void NotifyOnCancel(google::protobuf::Closure* callback) override;

private:
    // 调用失败状态
    bool failed_;

    // 调用错误信息
    std::string error_text_;
};

} // namespace tinyrpc
