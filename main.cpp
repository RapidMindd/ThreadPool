#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>
#include <vector>

constexpr int THREADS_COUNT = 8;
constexpr int BATCH_SIZE = 10'000;
constexpr int TASKS_COUNT = 1'000'000;

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
  explicit th_pool_t(size_t count);
  ~th_pool_t();
  void submit(f_t f);
  void pause();
  void resume();
  void wait();
private:
  void execute();
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
    return tasks_.empty() && active == 0;
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

th_pool_t::th_pool_t(size_t count)
{
  auto lambda = [this]()
  {
    execute();
  };
  for (size_t i = 0; i < count; ++i) {
    threads_.emplace_back(lambda);
  }
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
}

void th_pool_t::execute()
{
  std::queue< f_t > local_tasks;
  std::unique_lock lck(mutex_);
  do {
    auto proceed = [&]
    {
      return stop_ || (!paused_ && !tasks_.empty());
    };
    cv_.wait(lck, proceed);
    if (tasks_.empty()) {
      continue;
    }
    for (size_t i = 0; i < BATCH_SIZE && !tasks_.empty(); ++i) {
      local_tasks.push(std::move(tasks_.front()));
      tasks_.pop();
    }
    ++active;
    lck.unlock();
    while (!local_tasks.empty()) {
      auto task = std::move(local_tasks.front());
      local_tasks.pop();
      try {
        task();
      } catch (...) {
      }
    }
    lck.lock();
    --active;
    if (tasks_.empty() && active == 0) {
      wait_cv_.notify_all();
    }
  } while (!stop_ || !tasks_.empty());
}

int main()
{
  std::vector< size_t > results(TASKS_COUNT);
  auto make_fib = [&results](size_t n, size_t index)
  {
    return [n, index, &results]()
    {
      if (n <= 1) {
        results[index] = n;
        return;
      }
      size_t a = 0, b = 1;
      for (size_t i = 2; i <= n; ++i) {
        size_t c = a + b;
        a = b;
        b = c;
      }
      results[index] = b;
    };
  };
  std::cout << "started\n";
  th_pool_t pool(THREADS_COUNT);
  pool.pause();
  for (size_t i = 1; i < TASKS_COUNT; ++i) {
    pool.submit(make_fib((i * 10 + i) % 90, i));
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
