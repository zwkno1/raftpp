#pragma once

#include <memory>
#include <optional>
#include <variant>

#include <raftpp/detail/error.h>
#include <raftpp/detail/utils.h>
namespace raft {

template <typename T = void, typename Err = ErrorCode>
class Result
{
public:
    template <typename... Args>
    Result(Args&&... args)
      : value_(std::forward<Args>(args)...)
    {
    }

    Result(Err e)
      : value_(std::move(e))
    {
    }

    // inline operator bool() const { return has_value(); }

    inline T& operator*() { return std::get<T>(value_); }

    inline const T& operator*() const { return std::get<T>(value_); }

    inline T* operator->() { return std::addressof(std::get<T>(value_)); }

    inline const T* operator->() const { return std::addressof(std::get<T>(value_)); }

    inline Err& error() { return std::get<Err>(value_); }

    inline const Err& error() const { return std::get<Err>(value_); }

    inline T& value() & { return std::get<T>(value_); }

    inline const T& value() const& { return std::get<T>(value_); }

    inline T&& value() && { return std::move(std::get<T>(value_)); }

    inline bool has_value() const { return std::holds_alternative<T>(value_); }

    inline bool has_error() const { return !has_value(); }

    inline T value_or(T&& v)
    {
        if (has_value()) {
            return value();
        }
        return v;
    }

    inline T& unwrap() &
    {
        if (has_error()) {
            panic(error());
        }
        return value();
    }

    inline const T& unwrap() const&
    {
        if (has_error()) {
            panic(error());
        }
        return value();
    }

    inline T&& unwrap() &&
    {

        if (has_error()) {
            panic(error());
        }
        return std::move(value());
    }

private:
    std::variant<T, Err> value_;
};

template <typename Err>
class Result<void, Err>
{
public:
    Result() {}

    Result(Err e)
      : value_(std::move(e))
    {
    }

    // inline operator bool() const { return !value_.has_value(); }

    inline const Err& error() const { return *value_; }

    inline Err& error() { return *value_; }

    inline bool has_value() const { return !value_.has_value(); }

    inline bool has_error() const { return !has_value(); }

    void unwrap() const
    {
        if (has_error()) {
            panic(error());
        }
    }

private:
    std::optional<Err> value_;
};

template <typename T>
class Result<T, void>
{
public:
    Result() {}

    Result(T t)
      : value_(std::move(t))
    {
    }

    // inline operator bool() const { return value_.has_value(); }

    inline T& operator*() { return value_.value(); }

    inline const T& operator*() const { return value_.value(); }

    inline T* operator->() { return std::addressof(std::get<T>(value_)); }

    inline const T* operator->() const { return std::addressof(std::get<T>(value_)); }

    inline bool has_value() const { return value_.has_value(); }

    inline bool has_error() const { return !has_value(); }

    inline T& value() & { return std::get<T>(value_); }

    inline const T& value() const& { return std::get<T>(value_); }

    inline T&& value() && { return std::move(std::get<T>(value_)); }

    T& unwrap() &
    {
        if (has_error()) {
            panic();
        }
        return value();
    }

    const T& unwrap() const&
    {
        if (has_error()) {
            panic();
        }
        return value();
    }

    T&& unwrap() &&
    {
        if (has_error()) {
            panic();
        }
        return std::move(value());
    }

    inline T value_or(T&& v)
    {
        if (has_value()) {
            return value();
        }
        return v;
    }

private:
    std::optional<T> value_;
};

} // namespace raft