#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>
#include <pthread.h>

constexpr int THREADS_COUNT = 16;
constexpr int BATCH_SIZE = 10;
constexpr int TASKS_COUNT = 500;
constexpr bool STEALING_ENABLED = true;

size_t fib(size_t n)
{
  if (n <= 1) {
    return n;
  }
  return fib(n - 1) + fib(n - 2);
}

struct task_t
{
  void operator()()
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    std::cout << "hello\n";
  }
};

struct th_pool_t
{
  using f_t = std::function< void(void) >;
  explicit th_pool_t(size_t count, bool stealing_switch = true);
  ~th_pool_t();
  void submit(f_t f);
  void pause();
  void resume();
  void wait();
private:
  void execute(size_t thread_id);
  std::vector< std::queue< f_t > > local_queues_;
  std::vector< pthread_rwlock_t > local_queue_mutexes_;
  bool has_tasks();
  bool stealing_enabled_;
  std::queue< f_t > tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
  size_t active = 0;
  std::condition_variable wait_cv_;
  bool stop_ = false;
  bool paused_ = false;
  std::vector< std::thread > threads_;
};

void th_pool_t::wait()
{
  std::unique_lock lck(mutex_);
  auto proceed = [this]
  {
    return !has_tasks() && active == 0;
  };
  wait_cv_.wait(lck, proceed);
}

void th_pool_t::pause()
{
  mutex_.lock();
  paused_ = true;
  mutex_.unlock();
  cv_.notify_all();
}

void th_pool_t::resume()
{
  mutex_.lock();
  paused_ = false;
  mutex_.unlock();
  cv_.notify_all();
}

void th_pool_t::submit(f_t f)
{
  mutex_.lock();
  tasks_.push(std::move(f));
  mutex_.unlock();
  cv_.notify_one();
}

th_pool_t::th_pool_t(size_t count, bool stealing_switch):
  local_queues_(count),
  local_queue_mutexes_(count),
  stealing_enabled_(stealing_switch)
{
  for (auto& lock: local_queue_mutexes_) {
    pthread_rwlock_init(&lock, nullptr);
  }
  for (size_t i = 0; i < count; ++i) {
    threads_.emplace_back(
        [this, i]
        {
          execute(i);
        });
  }
}

bool th_pool_t::has_tasks()
{
  if (!tasks_.empty()) {
    return true;
  }
  for (const auto& queue: local_queues_) {
    if (!queue.empty()) {
      return true;
    }
  }
  return false;
}

th_pool_t::~th_pool_t()
{
  mutex_.lock();
  stop_ = true;
  mutex_.unlock();
  cv_.notify_all();
  for (auto&& thread: threads_) {
    thread.join();
  }
  for (auto& lock: local_queue_mutexes_) {
    pthread_rwlock_destroy(&lock);
  }
}

void th_pool_t::execute(size_t thread_id)
{
  std::unique_lock lck(mutex_);
  auto& local_tasks = local_queues_[thread_id];
  auto& local_mutex = local_queue_mutexes_[thread_id];
  do {
    auto proceed = [&]
    {
      bool available = !local_tasks.empty() || !tasks_.empty();
      if (stealing_enabled_) {
        available = has_tasks();
      }
      return stop_ || (!paused_ && available);
    };
    cv_.wait(lck, proceed);
    if (!has_tasks()) {
      continue;
    }
    if (local_tasks.empty()) {
      pthread_rwlock_wrlock(&local_mutex);
      for (size_t i = 0; i < BATCH_SIZE && !tasks_.empty(); ++i) {
        local_tasks.push(std::move(tasks_.front()));
        tasks_.pop();
      }
      cv_.notify_all();
      pthread_rwlock_unlock(&local_mutex);
    }
    if (stealing_enabled_ && local_tasks.empty()) {
      for (size_t i = 0; i < local_queues_.size(); ++i) {
        if (i == thread_id) {
          continue;
        }
        auto& queue = local_queues_[i];
        auto& queue_mutex = local_queue_mutexes_[i];
        lck.unlock();
        pthread_rwlock_rdlock(&queue_mutex);
        bool available = !queue.empty();
        pthread_rwlock_unlock(&queue_mutex);
        lck.lock();
        if (paused_ && !stop_) {
          break;
        }
        if (!available) {
          continue;
        }
        pthread_rwlock_wrlock(&queue_mutex);
        if (!queue.empty()) {
          pthread_rwlock_wrlock(&local_mutex);
          size_t count = queue.size() / 2 + queue.size() % 2;
          for (size_t j = 0; j < count; ++j) {
            local_tasks.push(std::move(queue.front()));
            queue.pop();
          }
          pthread_rwlock_unlock(&local_mutex);
          pthread_rwlock_unlock(&queue_mutex);
          break;
        }
        pthread_rwlock_unlock(&queue_mutex);
      }
      if (local_tasks.empty()) {
        continue;
      }
    }
    ++active;
    while (!local_tasks.empty()) {
      pthread_rwlock_wrlock(&local_mutex);
      auto task = std::move(local_tasks.front());
      local_tasks.pop();
      pthread_rwlock_unlock(&local_mutex);
      lck.unlock();
      try {
        task();
      } catch (...) {
      }
      lck.lock();
    }
    --active;
    if (!has_tasks() && active == 0) {
      wait_cv_.notify_all();
    }
  } while (!stop_ || has_tasks());
}

int main()
{
  std::vector< size_t > results(TASKS_COUNT);
  auto make_fib = [&results](size_t n, size_t index)
  {
    return [n, index, &results]()
    {
      results[index] = fib(n);
    };
  };
  std::cout << "started\n";
  th_pool_t pool(THREADS_COUNT, STEALING_ENABLED);
  pool.pause();
  for (size_t i = 0; i < TASKS_COUNT; ++i) {
    size_t n = i < BATCH_SIZE ? 40 : 20;
    pool.submit(make_fib(n, i));
  }
  const auto start = std::chrono::steady_clock::now();
  pool.resume();
  pool.wait();
  const auto end = std::chrono::steady_clock::now();
  const auto duration = std::chrono::duration_cast< std::chrono::milliseconds >(end - start);
  std::cout << "finished\n";
  std::cout << std::accumulate(results.begin(), results.end(), size_t(0)) << '\n';
  std::cout << duration.count() << '\n';
}
