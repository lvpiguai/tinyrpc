cmake_minimum_required(VERSION 3.10)

project(tinyrpc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 查找 Protobuf 和线程库
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

# 添加头文件搜索路径
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
)

# 列出要编译的 proto 文件
set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/calculator.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_header.proto
)

# 把 proto 生成 C++ 代码
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# 打包 RPC 基础代码
add_library(tinyrpc_lib
    src/tcp_socket.cpp
    src/rpc_channel.cpp
    src/rpc_provider.cpp
    src/rpc_controller.cpp
    src/rpc_codec.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS}
)

target_link_libraries(tinyrpc_lib
    protobuf::libprotobuf
    Threads::Threads
)

# 生成服务端程序
add_executable(server
    example/server.cpp
)

target_link_libraries(server
    tinyrpc_lib
)

# 生成客户端程序
add_executable(client
    example/client.cpp
)

target_link_libraries(client
    tinyrpc_lib
)
