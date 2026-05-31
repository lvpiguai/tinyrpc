#include "rpc_controller.h"

namespace tinyrpc {

RpcController::RpcController()
    : failed_(false) {}

void RpcController::Reset() {
    failed_ = false;
    error_text_.clear();
}

bool RpcController::Failed() const {
    return failed_;
}

std::string RpcController::ErrorText() const {
    return error_text_;
}

void RpcController::StartCancel() {
    // 第一版暂不支持取消 RPC
}

void RpcController::SetFailed(const std::string& reason) {
    failed_ = true;
    error_text_ = reason;
}

bool RpcController::IsCanceled() const {
    return false;
}

void RpcController::NotifyOnCancel(google::protobuf::Closure* callback) {
    // 第一版暂不支持取消回调
}

} // namespace tinyrpc