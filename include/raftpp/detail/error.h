#pragma once

#include <exception>
#include <stdexcept>
#include <string>
#include <type_traits>

#include <spdlog/spdlog.h>
namespace raft {

enum ErrorCode : uint32_t
{
    // ErrCompacted is returned by Storage.Entries/Compact when a requested
    // index is unavailable because it predates the last snapshot.
    ErrCompacted = 1,

    // ErrSnapOutOfDate is returned by Storage.CreateSnapshot when a requested
    // index is older than the existing snapshot.
    ErrSnapOutOfDate,

    // ErrUnavailable is returned by Storage interface when the requested log entries
    // are unavailable.
    ErrUnavailable,

    // ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
    // snapshot is temporarily unavailable.
    // var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")
    ErrSnapshotTemporarilyUnavailable,

    // ErrProposalDropped is returned when the proposal is ignored by some cases,
    // so that the proposer can be notified and fail fast.
    ErrProposalDropped,

    // ErrStepPeerNotFound is returned when try to step a response message
    // but there is no peer found in raft.trk for that node.
    ErrStepPeerNotFound,

    // ErrBreak is used for break scan
    ErrLogScanBreak,
    ErrLogScanEmpty,

};

class Error : public std::exception
{
public:
    Error(std::string msg)
      : what_(std::move(msg))
    {
    }

    template <typename... Args>
    inline static Error fmt(fmt::format_string<Args...> fmt, Args&&... args)
    {
        return Error{ fmt::format(fmt, std::forward<Args>(args)...) };
    }

    const char* what() const noexcept { return what_.c_str(); }

private:
    std::string what_;
};

[[noreturn]] inline void panic()
{
    throw Error{ "empty error" };
}

template <typename T>
[[noreturn]] inline void panic(T reason)
{
    if constexpr (std::is_same_v<T, Error>) {
        throw reason;
    } else if constexpr (std::is_same_v<T, ErrorCode>) {
        throw Error::fmt("error code: {}", uint64_t{ reason });
    } else if (std::is_convertible_v<T, std::string>) {
        throw Error{ reason };
    } else {
        throw Error{ "unknown error" };
    }
}

template <typename... Args>
[[noreturn]] inline void panic(fmt::format_string<Args...> fmt, Args&&... args)
{
    throw Error::fmt(fmt, std::forward<Args>(args)...);
}

} // namespace raft