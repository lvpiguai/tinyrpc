cmake_minimum_required(VERSION 3.10)

project(tinyrpc LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

set(RPC_PROTO_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_header.proto
)

set(CALCULATOR_PROTO_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/examples/calculator/calculator.proto
)

protobuf_generate_cpp(RPC_PROTO_SRCS RPC_PROTO_HDRS ${RPC_PROTO_FILE})
protobuf_generate_cpp(CALCULATOR_PROTO_SRCS CALCULATOR_PROTO_HDRS ${CALCULATOR_PROTO_FILE})

add_library(tinyrpc_lib STATIC
    src/common/tcp_socket.cpp
    src/common/protocol_codec.cpp
    src/common/rpc_transport.cpp
    src/common/rpc_controller.cpp
    src/client/rpc_channel.cpp
    src/server/rpc_provider.cpp
    src/server/thread_pool.cpp
    src/common/registry_client.cpp
    ${RPC_PROTO_SRCS}
    ${RPC_PROTO_HDRS}
)

target_include_directories(tinyrpc_lib
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(tinyrpc_lib
    PUBLIC
        protobuf::libprotobuf
        Threads::Threads
)

add_library(calculator_proto STATIC
    ${CALCULATOR_PROTO_SRCS}
    ${CALCULATOR_PROTO_HDRS}
)

target_include_directories(calculator_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(calculator_proto
    PUBLIC
        protobuf::libprotobuf
)

add_executable(calculator_server
    examples/calculator/server.cpp
)

target_link_libraries(calculator_server
    PRIVATE
        tinyrpc_lib
        calculator_proto
)

add_executable(calculator_client
    examples/calculator/client.cpp
)

target_link_libraries(calculator_client
    PRIVATE
        tinyrpc_lib
        calculator_proto
)

add_executable(tinyrpc_registry
    registry/main.cpp
)

target_link_libraries(tinyrpc_registry
    PRIVATE
        tinyrpc_lib
)
