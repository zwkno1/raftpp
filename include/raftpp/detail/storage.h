#pragma once

#include <raftpp/detail/message.h>
#include <raftpp/detail/result.h>

namespace raft {

using SnapshotPtr = std::shared_ptr<Snapshot>;

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.

template <typename T>
concept Storage = requires(T t) {
    // InitialState returns the saved HardState and ConfState information.
    // Result<State> initialState()
    {
        t.initialState()
    } -> std::same_as<Result<State>>;

    // Entries returns a slice of consecutive log entries in the range [lo, hi),
    // starting from lo. The maxSize limits the total size of the log entries
    // returned, but Entries returns at least one entry if any.
    //
    // The caller of Entries owns the returned slice, and may append to it. The
    // individual entries in the slice must not be mutated, neither by the Storage
    // implementation nor the caller. Note that raft may forward these entries
    // back to the application via Ready struct, so the corresponding handler must
    // not mutate entries either (see comments in Ready struct).
    //
    // Since the caller may append to the returned slice, Storage implementation
    // must protect its state from corruption that such appends may cause. For
    // example, common ways to do so are:
    //  - allocate the slice before returning it (safest option),
    //  - return a slice protected by Go full slice expression, which causes
    //  copying on appends (see MemoryStorage).
    //
    // Returns ErrCompacted if entry lo has been compacted, or ErrUnavailable if
    // encountered an unavailable entry in [lo, hi).
    // Result<EntryList> entries(Index lo, Index hi, size_t maxSize)
    {
        t.entries(Index{}, Index{}, size_t{})
    } -> std::same_as<Result<EntryList>>;

    // Term returns the term of entry i, which must be in the range
    // [FirstIndex()-1, LastIndex()]. The term of the entry before
    // FirstIndex is retained for matching purposes even though the
    // rest of that entry may not be available.
    // Result<Term> term(Index i)
    {
        t.term(Index{})
    } -> std::same_as<Result<Term>>;

    // FirstIndex returns the index of the first log entry that is
    // possibly available via Entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    // Result<Term> firstIndex()
    {
        t.firstIndex()
    } -> std::same_as<Result<Term>>;

    // LastIndex returns the index of the last entry in the log.
    // Result<Term> lastIndex()
    {
        t.lastIndex()
    } -> std::same_as<Result<Term>>;

    // Snapshot returns the most recent snapshot.
    // If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
    // so raft state machine could know that Storage needs some time to prepare
    // snapshot and call Snapshot later.
    // Result<SnapshotPtr> snapshot()
    {
        t.snapshot()
    } -> std::same_as<Result<SnapshotPtr>>;
};

} // namespace raft