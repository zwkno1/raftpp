#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <vector>

#include <error.h>
#include <result.h>
#include <storage.h>
#include <utils.h>

#include <raftpb/raft.pb.h>

namespace raft {

// unstable contains "unstable" log entries and snapshot state that has
// not yet been written to Storage. The type serves two roles. First, it
// holds on to new log entries and an optional snapshot until they are
// handed to a Ready struct for persistence. Second, it continues to
// hold on to this state after it has been handed off to provide raftLog
// with a view of the in-progress log entries and snapshot until their
// writes have been stabilized and are guaranteed to be reflected in
// queries of Storage. After this point, the corresponding log entries
// and/or snapshot can be cleared from unstable.
//
// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
class Unstable
{
public:
    Unstable()
      : offset_(0)
      , offsetInProgress_(0)
      , snapshotInProgress_(false)
    {
    }

    // maybeFirstIndex returns the index of the first possible entry in entries
    // if it has a snapshot.
    std::optional<Index> firstIndex() const
    {
        if (snapshot_) {
            return snapshot_->metadata().index() + 1;
        }
        return {};
    }

    // maybeLastIndex returns the last index if it has at least one
    // unstable entry or snapshot.
    std::optional<Index> lastIndex() const
    {
        if (!entries_.empty()) {
            return offset_ + (entries_.size() - 1);
        }

        if (snapshot_) {
            return snapshot_->metadata().index();
        }
        return {};
    }

    // maybeTerm returns the term of the entry at index i, if there
    // is any.
    std::optional<Index> term(Index i) const
    {
        if (i < offset_) {
            if (snapshot_ && (snapshot_->metadata().index() == i)) {
                return snapshot_->metadata().term();
            }
        } else if (i < offset_ + entries_.size()) {
            return entries_[i - offset_].term();
        }

        return {};
    }

    // nextEntries returns the unstable entries that are not already in the process
    // of being written to storage.
    EntryList nextEntries() const
    {
        auto start = offsetInProgress_ - offset_;
        return { entries_.begin() + start, entries_.end() };
    }

    bool hasNextEntries() const { return offsetInProgress_ < offset_ + entries_.size(); }

    SnapshotPtr nextSnapshot() const
    {
        if (!snapshot_ || snapshotInProgress_) {
            return nullptr;
        }
        return snapshot_;
    }

    void acceptInprogress()
    {
        if (!entries_.empty()) {
            offsetInProgress_ = offset_ + entries_.size();
        }
        if (snapshot_) {
            snapshotInProgress_ = true;
        }
    }

    // stableTo marks entries up to the entry with the specified (index, term) as
    // being successfully written to stable storage.
    //
    // The method should only be called when the caller can attest that the entries
    // can not be overwritten by an in-progress log append. See the related comment
    // in newStorageAppendRespMsg.
    void stableEntries(Index i, Term t)
    {
        if (i < offset_) {
            return;
        }

        auto gt = term(i);
        if (!gt) {
            return;
        }

        if (*gt != t) {
            return;
        }

        auto num = i + 1 - offset_;
        entries_.erase(entries_.begin(), entries_.begin() + num);
        offset_ = i + 1;
        offsetInProgress_ = std::max(offsetInProgress_, offset_);
    }

    void stableSnapshot(Index i)
    {
        if (snapshot_ && (snapshot_->metadata().index() == i)) {
            snapshot_ = nullptr;
            snapshotInProgress_ = false;
        }
    }

    void restore(std::shared_ptr<pb::Snapshot> s)
    {
        offset_ = s->metadata().index() + 1;
        offsetInProgress_ = offset_;
        entries_.clear();

        snapshot_ = s;
        snapshotInProgress_ = false;
    }

    void appendEntries(EntrySlice ents)
    {
        auto after = ents[0].index();

        if (after <= offset_) {
            //  The log is being truncated to before our current offset
            //  portion, so set the offset and replace the entries.
            offset_ = after;
            entries_.clear();
        } else if (after <= offset_ + entries_.size()) {
            // Truncate to fromIndex (exclusive), and append the new entries.
            // if after == offset_ + entries_.size() resize do nothing.
            entries_.resize(after - offset_);
        } else {
            panic("unstable out of bound");
        }
        entries_.append_range(ents);
    }

    // slice returns the entries from the unstable log with indexes in the range
    // [lo, hi). The entire range must be stored in the unstable log or the method
    // will panic. The returned slice can be appended to, but the entries in it must
    // not be changed because they are still shared with unstable.
    //
    // TODO(pavelkalinnikov): this, and similar []pb.Entry slices, may bubble up all
    // the way to the application code through Ready struct. Protect other slices
    // similarly, and document how the client can use them.
    EntryView<std::vector<pb::Entry>::const_iterator> slice(Index lo, Index hi, size_t limit) const
    {
        if (lo >= hi) {
            panic("bad index range");
        }
        auto upper = offset_ + entries_.size();

        if (lo < offset_ || hi > upper) {
            panic("index out of range");
        }

        size_t bytes = payloadsSize(entries_[lo - offset_]);
        size_t i = lo + 1;
        for (; i != hi; ++i) {
            if (bytes > limit) {
                break;
            }
            bytes += payloadsSize(entries_[i - offset_]);
        }

        // NB: use the full slice expression to limit what the caller can do with the
        // returned slice. For example, an append will reallocate and copy this slice
        // instead of corrupting the neighbouring entries_.
        return { entries_.begin() + (lo - offset_), entries_.begin() + (i - offset_) };
    }

    void init(Index offset, EntryList entries = {})
    {
        offset_ = offset;
        offsetInProgress_ = offset_;
        entries_ = entries;
    }

    inline Index offset() const { return offset_; }

    inline SnapshotPtr snapshot() const { return snapshot_; }

    inline bool hasEntries() const { return entries_.empty(); }

    // const EntryList & entries() const { return entries_; }

    // test
    inline void setSnapshot(SnapshotPtr snapshot) { snapshot_ = snapshot; }

    inline const EntryList& entries() const { return entries_; }

private:
    // shrinkEntriesArray discards the underlying array used by the entries slice
    // if most of it isn't being used. This avoids holding references to a bunch of
    // potentially large entries that aren't needed anymore. Simply clearing the
    // entries wouldn't be safe because clients might still be using them.
    void shrinkEntries()
    {
        // We replace the array if we're using less than half of the space in
        // it. This number is fairly arbitrary, chosen as an attempt to balance
        // memory usage vs number of allocations. It could probably be improved
        // with some focused tuning.
        constexpr size_t lenMultiple = 2;
        if (entries_.empty() || (entries_.size() * lenMultiple < entries_.capacity())) {
            entries_.shrink_to_fit();
        }
    }

    // entries[i] has raft log position i+offset.
    Index offset_;

    // entries[:offsetInProgress-offset] are being written to storage.
    // Like offset, offsetInProgress is exclusive, meaning that it
    // contains the index following the largest in-progress entry.
    // Invariant: offset <= offsetInProgress
    Index offsetInProgress_;

    // if true, snapshot is being written to storage.
    bool snapshotInProgress_;

    // the incoming unstable snapshot, if any.
    SnapshotPtr snapshot_;

    // all entries that have not yet been written to storage.
    EntryList entries_;
};

} // namespace raft