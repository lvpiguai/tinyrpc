#pragma once

#include "rpc_header.pb.h"

#include <cstdint>
#include <string>

namespace tinyrpc {

class TcpSocket;

enum RpcResponseStatusCode {
    RPC_OK = 0,
    RPC_INVALID_REQUEST = 1,
    RPC_SERVICE_NOT_FOUND = 2,
    RPC_METHOD_NOT_FOUND = 3,
    RPC_PARSE_REQUEST_FAILED = 4,
    RPC_SERIALIZE_RESPONSE_FAILED = 5,
    RPC_INTERNAL_ERROR = 6
};

enum class RpcCodecStatus {
    OK = 0,
    SOCKET_ERROR,
    HEADER_TOO_LARGE,
    BODY_TOO_LARGE,
    HEADER_PARSE_FAILED,
    BODY_SIZE_MISMATCH,
    HEADER_SERIALIZE_FAILED
};

const char* rpcCodecStatusToString(RpcCodecStatus status);

// 负责按 TinyRPC 协议收发 RPC 消息字节
class RpcCodec {
public:
    static RpcCodecStatus sendRequest(TcpSocket& socket,
                                      const RpcRequestHeader& header,
                                      const std::string& request_body);

    static RpcCodecStatus recvRequest(TcpSocket& socket,
                                      RpcRequestHeader& header,
                                      std::string& request_body);

    static RpcCodecStatus sendResponse(TcpSocket& socket,
                                       const RpcResponseHeader& header,
                                       const std::string& response_body);

    static RpcCodecStatus recvResponse(TcpSocket& socket,
                                       RpcResponseHeader& header,
                                       std::string& response_body);
};

} // namespace tinyrpc
