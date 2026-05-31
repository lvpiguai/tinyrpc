cmake_minimum_required(VERSION 3.10)

project(tinyrpc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
)

set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/calculator.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_header.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(tinyrpc_lib
    src/tcp_socket.cpp
    src/rpc_channel.cpp
    src/rpc_provider.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

target_link_libraries(tinyrpc_lib
    protobuf::libprotobuf
)

add_executable(server
    example/server.cpp
)

target_link_libraries(server
    tinyrpc_lib
)

add_executable(client
    example/client.cpp
)

target_link_libraries(client
    tinyrpc_lib
)