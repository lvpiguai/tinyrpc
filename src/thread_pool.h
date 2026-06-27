#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace tinyrpc {

// 固定大小线程池，用于限制并发执行的任务数量
class ThreadPool {
public:
    // worker_count 为工作线程数，max_queue_size 为等待队列容量
    ThreadPool(size_t worker_count, size_t max_queue_size);
    ~ThreadPool();

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // 队列满或线程池停止时返回 false
    bool submit(std::function<void()> task);

private:
    // 工作线程主循环
    void workerLoop();

private:
    // 工作线程集合
    std::vector<std::thread> workers_;

    // 等待执行的任务队列
    std::queue<std::function<void()>> tasks_;
    size_t max_queue_size_;

    // 停止标记，置为 true 后不再接收新任务
    bool stopped_ = false;

    // 保护任务队列和停止标记
    std::mutex mutex_;

    // 用于通知 worker 有新任务或线程池停止
    std::condition_variable cv_;
};

} // namespace tinyrpc
