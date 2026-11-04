#include <iostream>
#include <thread>

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
private:
  void execute();
  std::queue< f_t > tasks_;
  std::mutex mutex_;
  std::condition_variable cv_;
  bool stop_ = false;
  std::vector< std::thread > threads_;
};

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
  std::unique_lock lck(mutex_);
  do {
    auto proceed = [&]
    {
      return !tasks_.empty() || stop_;
    };
    cv_.wait(lck, proceed);
    if (tasks_.empty()) {
      continue;
    }
    auto task = std::move(tasks_.front());
    tasks_.pop();
    lck.unlock();
    try {
      task();
    } catch (...) {
    }
    lck.lock();
  } while (!stop_ || !tasks_.empty());
}

int main()
{
  th_pool_t pool(4);
  for (int i = 0; i < 10; ++i) {
    pool.submit(task_t{});
  }
}
