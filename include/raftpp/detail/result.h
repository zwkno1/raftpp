#pragma once

#include <expected>

#include <raftpp/detail/error.h>
namespace raft {

template <typename T = void, typename Err = ErrorCode>
class Result : public std::expected<T, Err>
{
public:
    template <typename... Args>
    Result(Args&&... args)
      : std::expected<T, Err>(std::forward<Args>(args)...)
    {
    }

    Result(Err e)
      : std::expected<T, Err>(std::unexpected(e))
    {
    }

    const T& unwrap() const&
    {
        if (!this->has_value()) {
            throw this->error();
        }
        return this->value();
    }

    T& unwrap() &
    {
        if (!this->has_value()) {
            throw this->error();
        }
        return this->value();
    }

    T&& unwrap() &&
    {
        if (!this->has_value()) {
            throw this->error();
        }
        return std::move(this->value());
    }
};

template <typename Err>
class Result<void, Err> : public std::expected<void, Err>
{
public:
    Result()
      : std::expected<void, Err>()
    {
    }

    Result(Err e)
      : std::expected<void, Err>(std::unexpected(e))
    {
    }

    void unwrap() const
    {
        if (!this->has_value()) {
            throw this->error();
        }
    }
};

} // namespace raft