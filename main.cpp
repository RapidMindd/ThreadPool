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
    auto task = std::move(tasks_.front());
    tasks_.pop();
    ++active;
    lck.unlock();
    try {
      task();
    } catch (...) {
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
  std::cout << "started\n";
  th_pool_t pool(4);
  pool.pause();
  for (int i = 0; i < 10; ++i) {
    pool.submit(task_t{});
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  pool.resume();
  pool.wait();
  std::cout << "finished\n";
}
