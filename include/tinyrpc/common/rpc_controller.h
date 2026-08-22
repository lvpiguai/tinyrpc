#pragma once

#include <google/protobuf/service.h>

#include <string>

namespace tinyrpc {

class RpcController : public google::protobuf::RpcController {
public:
    RpcController();

    void Reset() override;

    bool Failed() const override;

    std::string ErrorText() const override;

    void StartCancel() override;

    void SetFailed(const std::string& reason) override;

    bool IsCanceled() const override;

    void NotifyOnCancel(google::protobuf::Closure* callback) override;

private:
    bool failed_;
    std::string error_text_;
};

} // namespace tinyrpc