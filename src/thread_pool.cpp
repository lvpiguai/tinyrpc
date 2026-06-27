#include "thread_pool.h"

namespace tinyrpc {

ThreadPool::ThreadPool(size_t worker_count, size_t max_queue_size)
    : max_queue_size_(max_queue_size) {
    // 启动固定数量的工作线程
    workers_.reserve(worker_count);
    for (size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&ThreadPool::workerLoop, this);
    }
}

ThreadPool::~ThreadPool() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopped_ = true;
    }

    // 唤醒所有 worker，使其处理完剩余任务后退出
    cv_.notify_all();

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

bool ThreadPool::submit(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopped_ || tasks_.size() >= max_queue_size_) {
            return false;
        }

        tasks_.push(std::move(task));
    }

    // 唤醒一个等待任务的 worker
    cv_.notify_one();
    return true;
}

void ThreadPool::workerLoop() {
    while (true) {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            // 等待新任务或停止信号
            cv_.wait(lock, [this]() {
                return stopped_ || !tasks_.empty();
            });

            // 停止后仍会先处理完队列中已有任务
            if (stopped_ && tasks_.empty()) {
                return;
            }

            task = std::move(tasks_.front());
            tasks_.pop();
        }

        task();
    }
}

} // namespace tinyrpc
