#include <tinyrpc/common/protocol_codec.h>

#include <arpa/inet.h>

#include <cstdint>
#include <iostream>
#include <string>

namespace {

int failure_count = 0;

void expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        ++failure_count;
    }
}

void appendSize(std::string& frame, uint32_t size) {
    const auto network_size = htonl(size);
    frame.append(reinterpret_cast<const char*>(&network_size), sizeof(network_size));
}

void testRequestRoundTrip() {
    const tinyrpc::RpcRequest input{
        "tinyrpc.CalculatorService",
        "Add",
        std::string("request\0body", 12)
    };

    std::string frame;
    auto status = tinyrpc::protocol_codec::encode(input, frame);
    expect(status.ok(), "encode request");

    tinyrpc::RpcRequest output;
    status = tinyrpc::protocol_codec::decode(frame, output);
    expect(status.ok(), "decode request");
    expect(output.service_name == input.service_name, "preserve service name");
    expect(output.method_name == input.method_name, "preserve method name");
    expect(output.body == input.body, "preserve binary request body");
}

void testResponseRoundTrip() {
    const tinyrpc::RpcResponse input{
        false,
        "business failed",
        std::string("response\0body", 13)
    };

    std::string frame;
    auto status = tinyrpc::protocol_codec::encode(input, frame);
    expect(status.ok(), "encode response");

    tinyrpc::RpcResponse output;
    status = tinyrpc::protocol_codec::decode(frame, output);
    expect(status.ok(), "decode response");
    expect(output.success == input.success, "preserve response result");
    expect(output.error_message == input.error_message, "preserve error message");
    expect(output.body == input.body, "preserve binary response body");
}

void testIncompleteSizeField() {
    tinyrpc::RpcRequest request;
    const auto status = tinyrpc::protocol_codec::decode("abc", request);
    expect(status.code == tinyrpc::RpcErrorCode::HEADER_PARSE_FAILED,
           "reject incomplete header size");
}

void testOversizedHeader() {
    std::string frame;
    appendSize(frame,
               static_cast<uint32_t>(tinyrpc::protocol_codec::kMaxHeaderSize + 1));

    tinyrpc::RpcRequest request;
    const auto status = tinyrpc::protocol_codec::decode(frame, request);
    expect(status.code == tinyrpc::RpcErrorCode::HEADER_TOO_LARGE,
           "reject oversized header");
}

void testMalformedHeader() {
    std::string frame;
    appendSize(frame, 1);
    frame.push_back(static_cast<char>(0x80));

    tinyrpc::RpcRequest request;
    const auto status = tinyrpc::protocol_codec::decode(frame, request);
    expect(status.code == tinyrpc::RpcErrorCode::HEADER_PARSE_FAILED,
           "reject malformed protobuf header");
}

void testOversizedBody() {
    tinyrpc::RpcRequest request;
    request.service_name = "service";
    request.method_name = "method";
    request.body.resize(tinyrpc::protocol_codec::kMaxBodySize + 1);

    std::string frame;
    const auto status = tinyrpc::protocol_codec::encode(request, frame);
    expect(status.code == tinyrpc::RpcErrorCode::BODY_TOO_LARGE,
           "reject oversized body");
}

} // namespace

int main() {
    testRequestRoundTrip();
    testResponseRoundTrip();
    testIncompleteSizeField();
    testOversizedHeader();
    testMalformedHeader();
    testOversizedBody();

    if (failure_count != 0) {
        std::cerr << failure_count << " protocol codec test(s) failed\n";
        return 1;
    }

    std::cout << "all protocol codec tests passed\n";
    return 0;
}
