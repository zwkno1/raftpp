#pragma once

#include <raftpp/raftpp.h>

using namespace raft;

// MemoryStorage implements the Storage interface backed by an
// in-memory array.
class MemoryStorage
{
public:
    MemoryStorage()
    {
        // When starting from scratch populate the list with a dummy entry at term zero.
        ents_.resize(1);
        snapshot_ = std::make_shared<Snapshot>();
    }

    // InitialState implements the Storage interface.
    Result<State> initialState()
    {
        ++callStats_.initialState;
        return State{ hardState_, snapshot_->meta.confState };
    }

    // Entries implements the Storage interface.
    Result<EntryList> entries(Index lo, Index hi, size_t maxSize)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        ++callStats_.entries;
        auto offset = ents_[0].index;
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
        end = std::find_if(beg, end, [&](const Entry& e) -> bool {
            size += e.payload();
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

        auto offset = ents_[0].index;
        if (i < offset) {
            return ErrCompacted;
        }

        if (i - offset >= ents_.size()) {
            return ErrUnavailable;
        }
        return ents_[i - offset].term;
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
    Result<> setHardState(const HardState& st)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        hardState_ = st;
        return {};
    }

    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    Result<void> applySnapshot(Snapshot snap)
    {
        std::unique_lock<std::mutex> l{ mutex_ };

        // handle check for old snapshot being applied
        auto msIndex = snapshot_->meta.index;
        auto snapIndex = snap.meta.index;
        if (msIndex >= snapIndex) {
            return ErrSnapOutOfDate;
        }

        snapshot_ = std::make_shared<Snapshot>(snap);

        Entry e;
        e.index = snapshot_->meta.index;
        e.term = snapshot_->meta.term;

        ents_ = { std::move(e) };

        return {};
    }

    // CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
    // can be used to reconstruct the state at that point.
    // If any configuration changes have been made since the last compaction,
    // the result of the last ApplyConfChange must be passed in.
    Result<SnapshotPtr> createSnapshot(Index i, const ConfState* cs, std::string data)
    {
        std::unique_lock<std::mutex> l{ mutex_ };

        if (i <= snapshot_->meta.index) {
            return ErrSnapOutOfDate;
        }

        auto offset = ents_[0].index;

        if (i > last()) {
            panic("snapshot {} is out of bound lastindex({})", i, last());
        }

        snapshot_->meta.index = i;
        snapshot_->meta.term = ents_[i - offset].term;

        if (cs) {
            snapshot_->meta.confState = *cs;
        }
        snapshot_->data = std::move(data);
        return snapshot_;
    }

    // Compact discards all log entries prior to compactIndex.
    // It is the application's responsibility to not attempt to compact an index
    // greater than raftLog.applied.
    Result<void> compact(Index compactIndex)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        auto offset = ents_[0].index;
        if (compactIndex <= offset) {
            return ErrCompacted;
        }

        if (compactIndex > last()) {
            panic("compact {} is out of bound lastindex({})", compactIndex, last());
        }

        auto i = compactIndex - offset;
        // NB: allocate a new slice instead of reusing the old ents_. Entries in
        // ents_ are immutable, and can be referenced from outside MemoryStorage
        // through slices returned by ms.entries.

        ents_.erase(ents_.begin(), ents_.begin() + i);

        return {};
    }

    // Append the new entries to storage.
    // TODO (xiangli): ensure the entries are continuous and
    // entries[0].index > ms.entries[0].index
    Result<> append(const EntryList& entries)
    {
        if (entries.empty()) {
            return {};
        }

        std::unique_lock<std::mutex> l{ mutex_ };

        auto lo = first();
        auto hi = entries.back().index;

        // shortcut if there is no new entry.
        if (hi < lo) {
            return {};
        }

        // missing log entry
        if (last() + 1 < entries[0].index) {
            panic("missing log entry [last: {}, append at: {}]", last(), entries[0].index);
        }

        auto beg = entries.begin();
        // truncate compacted entries
        if (lo > entries[0].index) {
            beg += lo - entries[0].index;
        }

        ents_.erase(ents_.begin() + (beg->index - ents_[0].index), ents_.end());

        ents_.insert(ents_.end(), beg, entries.end());

        return {};
    }

private:
    inline Index first() const { return ents_.front().index + 1; }

    inline Index last() const { return ents_.back().index; }

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

    HardState hardState_;

    SnapshotPtr snapshot_;
    // ents[i] has raft log position i+snapshot.meta.index
    EntryList ents_;

    CallStats callStats_;
};