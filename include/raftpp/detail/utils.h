#pragma once

#include <random>
namespace raft {

template <typename T = int>
class RandomGenerator
{
public:
    explicit RandomGenerator(T beg = 0, T end = std::numeric_limits<T>::max())
      : dist_(beg, end)
      , engine_(std::random_device{}())
    {
    }

    inline T operator()() { return dist_(engine_); }

private:
    std::uniform_int_distribution<T> dist_;
    std::default_random_engine engine_;
};

} // namespace raft