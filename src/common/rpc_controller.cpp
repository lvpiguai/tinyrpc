#include <tinyrpc/common/rpc_controller.h>

namespace tinyrpc {

// 初始化调用状态
RpcController::RpcController()
    : failed_(false) {}

// 重置调用状态
void RpcController::Reset() {
    failed_ = false;
    error_text_.clear();
}

// 判断调用是否失败
bool RpcController::Failed() const {
    return failed_;
}

// 获取调用错误信息
std::string RpcController::ErrorText() const {
    return error_text_;
}

// 请求取消调用
void RpcController::StartCancel() {
    // 第一版暂不支持取消 RPC
}

// 标记调用失败
void RpcController::SetFailed(const std::string& reason) {
    failed_ = true;
    error_text_ = reason;
}

// 判断调用是否已取消
bool RpcController::IsCanceled() const {
    return false;
}

// 注册取消回调
void RpcController::NotifyOnCancel(google::protobuf::Closure* callback) {
    // 第一版暂不支持取消回调
}

} // namespace tinyrpc
