#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <span>
#include <type_traits>
#include <vector>

#include <error.h>
#include <result.h>
#include <utils.h>

#include <raftpb/raft.pb.h>

namespace raft {

struct State
{
    pb::HardState hard_;
    pb::ConfState conf_;
};

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
    // Result<EntryList> entries(Index lo, Index hi, uint64_t maxSize)
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

// using StoragePtr = std::shared_ptr<Storage>;

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
class MemoryStorage
{
public:
    MemoryStorage()
    {
        // When starting from scratch populate the list with a dummy entry at term zero.
        ents_.resize(1);
        snapshot_ = std::make_shared<pb::Snapshot>();
    }

    // InitialState implements the Storage interface.
    Result<State> initialState()
    {
        ++callStats_.initialState;
        return State{ hardState_, snapshot_->metadata().conf_state() };
    }

    // Entries implements the Storage interface.
    Result<EntryList> entries(Index lo, Index hi, uint64_t maxSize)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        ++callStats_.entries;
        auto offset = ents_[0].index();
        if (lo <= offset) {
            return ErrCompacted;
        }

        if (hi > last() + 1) {
            panic("entries' hi({}) is out of bound lastindex({})", hi, last());
        }

        // only contains dummy entries.
        if (ents_.size() == 1) {
            return ErrUnavailable;
        }

        auto beg = ents_.begin() + (lo - offset);
        auto end = ents_.begin() + (hi - offset);
        size_t size = 0;
        end = std::find_if(beg, end, [&](const pb::Entry& e) -> bool {
            size += e.ByteSizeLong();
            return size > maxSize;
        });

        // NB: use the full slice expression to limit what the caller can do with the
        // returned slice. For example, an append will reallocate and copy this slice
        // instead of corrupting the neighbouring ents_.
        return EntryList{ beg, end };
    }

    // Term implements the Storage interface.
    Result<Term> term(Index i)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        ++callStats_.term;

        auto offset = ents_[0].index();
        if (i < offset) {
            return ErrCompacted;
        }

        if (i - offset >= ents_.size()) {
            return ErrUnavailable;
        }
        return ents_[i - offset].term();
    }

    // LastIndex implements the Storage interface.
    Result<Index> lastIndex()
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        ++callStats_.lastIndex;
        return last();
    }

    // FirstIndex implements the Storage interface.
    Result<Index> firstIndex()
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        ++callStats_.firstIndex;
        return first();
    }

    // Snapshot implements the Storage interface.
    Result<SnapshotPtr> snapshot()
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        ++callStats_.snapshot;
        return snapshot_;
    }

    // SetHardState saves the current HardState.
    Result<> setHardState(const pb::HardState& st)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        hardState_ = st;
        return {};
    }

    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    Result<void> applySnapshot(pb::Snapshot snap)
    {
        std::unique_lock<std::mutex> l{ mutex_ };

        // handle check for old snapshot being applied
        auto msIndex = snapshot_->metadata().index();
        auto snapIndex = snap.metadata().index();
        if (msIndex >= snapIndex) {
            return ErrSnapOutOfDate;
        }

        snapshot_ = std::make_shared<pb::Snapshot>(snap);

        pb::Entry e;
        e.set_index(snapshot_->metadata().index());
        e.set_term(snapshot_->metadata().term());

        ents_ = { std::move(e) };

        return {};
    }

    // CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
    // can be used to reconstruct the state at that point.
    // If any configuration changes have been made since the last compaction,
    // the result of the last ApplyConfChange must be passed in.
    Result<SnapshotPtr> createSnapshot(uint64_t i, const pb::ConfState* cs, std::string data)
    {
        std::unique_lock<std::mutex> l{ mutex_ };

        if (i <= snapshot_->metadata().index()) {
            return ErrSnapOutOfDate;
        }

        auto offset = ents_[0].index();

        if (i > last()) {
            panic("snapshot {} is out of bound lastindex({})", i, last());
        }

        snapshot_->mutable_metadata()->set_index(i);
        snapshot_->mutable_metadata()->set_term(ents_[i - offset].term());

        if (cs) {
            *snapshot_->mutable_metadata()->mutable_conf_state() = *cs;
        }
        *snapshot_->mutable_data() = std::move(data);
        return snapshot_;
    }

    // Compact discards all log entries prior to compactIndex.
    // It is the application's responsibility to not attempt to compact an index
    // greater than raftLog.applied.
    Result<void> compact(uint64_t compactIndex)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        auto offset = ents_[0].index();
        if (compactIndex <= offset) {
            return ErrCompacted;
        }

        if (compactIndex > last()) {
            panic("compact {} is out of bound lastindex({})", compactIndex, last());
        }

        auto i = compactIndex - offset;
        // NB: allocate a new slice instead of reusing the old ents_. Entries in
        // ents_ are immutable, and can be referenced from outside MemoryStorage
        // through slices returned by ms.Entries().

        ents_.erase(ents_.begin(), ents_.begin() + i);

        return {};
    }

    // Append the new entries to storage.
    // TODO (xiangli): ensure the entries are continuous and
    // entries[0].index() > ms.entries[0].index()
    Result<> append(const EntryList& entries)
    {
        if (entries.empty()) {
            return {};
        }

        std::unique_lock<std::mutex> l{ mutex_ };

        auto lo = first();
        auto hi = entries.back().index();

        // shortcut if there is no new entry.
        if (hi < lo) {
            return {};
        }

        // missing log entry
        if (last() + 1 < entries[0].index()) {
            panic("missing log entry [last: {}, append at: {}]", last(), entries[0].index());
        }

        auto beg = entries.begin();
        // truncate compacted entries
        if (lo > entries[0].index()) {
            beg += lo - entries[0].index();
        }

        ents_.erase(ents_.begin() + (beg->index() - ents_[0].index()), ents_.end());

        ents_.insert(ents_.end(), beg, entries.end());

        return {};
    }

private:
    inline Index first() const { return ents_.front().index() + 1; }

    inline Index last() const { return ents_.back().index(); }

    struct CallStats
    {
        int initialState = 0;
        int firstIndex = 0;
        int lastIndex = 0;
        int entries = 0;
        int term = 0;
        int snapshot = 0;
    };

    // Protects access to all fields. Most methods of MemoryStorage are
    // run on the raft goroutine, but Append() is run on an application
    // goroutine.
    std::mutex mutex_;

    pb::HardState hardState_;

    SnapshotPtr snapshot_;
    // ents[i] has raft log position i+snapshot.metadata().index()
    EntryList ents_;

    CallStats callStats_;
};

} // namespace raft