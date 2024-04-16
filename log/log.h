#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <utility>

#include <__bit/countl.h>
#include <__memory/construct_at.h>
#include <__ranges/concepts.h>
#include <error.h>
#include <log/unstable.h>
#include <result.h>
#include <storage.h>
#include <utils.h>

#include <raftpb/raft.pb.h>

namespace raft {

template <Storage T>
class Log
{
public:
    // New returns log using the given storage and default options. It
    // recovers the log to the state that it just commits and applies the
    // latest snapshot.
    Log(T& storage, size_t maxApplyingEntsSize)
      : storage_(storage)
      , maxApplyingEntsSize_(maxApplyingEntsSize)
    {
        auto firstIndex = storage_.firstIndex().unwrap();
        auto lastIndex = storage_.lastIndex().unwrap();

        unstable_.init(lastIndex + 1);

        // Initialize our committed and applied pointers to the time of the last compaction.
        committed_ = firstIndex - 1;
        applying_ = firstIndex - 1;
        applied_ = firstIndex - 1;

        applyingEntsSize_ = 0;
    }

    Log(const Log&) = delete;
    Log& operator=(const Log&) = delete;

    inline T& storage() { return storage_; }

    /// Returns th first index in the store that is available via entries
    ///
    /// # Panics
    ///
    /// Panics if the store doesn't have a first index.
    inline Index firstIndex() const
    {
        auto index = unstable_.firstIndex();
        return index ? *index : storage_.firstIndex().unwrap();
    }

    /// lastIndex returns the last index in the store that is available via entries.
    /// Panics if the store doesn't have a last index.
    inline Index lastIndex() const
    {
        auto index = unstable_.lastIndex();
        return index ? *index : storage_.lastIndex().unwrap();
    }

    /// For a given index, finds the term associated with it.
    Result<Term> term(Index idx) const
    {
        // The valid term range is [firstIndex-1, lastIndex]. Even though the entry at
        // firstIndex-1 is compacted away, its term is available for matching purposes
        // when doing log appends.
        if (idx + 1 < firstIndex()) {
            return ErrCompacted;
        }

        if (idx > lastIndex()) {
            return ErrUnavailable;
        }

        // Check the unstable log first, even before computing the valid term range,
        // which may need to access stable Storage. If we find the entry's term in
        // the unstable log, we know it was in the valid range.
        auto t = unstable_.term(idx);
        if (t.has_value()) {
            return *t;
        }

        auto st = storage_.term(idx);
        if (st.has_value()) {
            return *st;
        }

        if (st.error() == ErrCompacted || st.error() == ErrUnavailable) {
            return st;
        }

        return st.unwrap();
    }

    /// Term returns the term from of last entry.
    /// Panics if there are entries but the last term has been discarded.
    inline Term lastTerm() const { return term(lastIndex()).unwrap(); }

    /// findConflictByTerm takes an (`index`, `term`) pair (indicating a conflicting log
    /// entry on a leader/follower during an append) and finds the largest index in
    /// log with log.term <= `term` and log.index <= `index`. If no such index exists
    /// in the log, the log's first index is returned.
    ///
    /// The index provided MUST be equal to or less than self.last_index(). Invalid
    /// inputs log a warning and the input index is returned.
    ///
    /// Return (index, term)
    IndexTerm findConflictByTerm(Index idx, Term t) const
    {
        for (; idx != 0; --idx) {
            // If there is an error (likely ErrCompacted or ErrUnavailable), we don't
            // know whether it's a match or not, so assume a possible match and return
            // the index, with 0 term indicating an unknown term.
            auto currentTerm = term(idx).value_or(0);
            if (currentTerm <= t) {
                return { idx, currentTerm };
            }
        }
        return { 0, 0 };
    }

    bool matchTerm(Index idx, Term t) const
    {
        auto currentTerm = term(idx);
        return currentTerm.has_value() && *currentTerm == t;
    }

    // maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
    // it returns (last index of new entries, true).
    std::optional<Index> maybeAppend(Index idx, Term t, Index committed, EntrySlice ents)
    {
        if (!matchTerm(idx, t)) {
            return {};
        }

        auto conflictIndex = findConflict(ents);

        if (conflictIndex == 0) {
        } else if (conflictIndex <= committed_) {
            panic("conflict with committed entry");
        } else {
            append(ents.sub(conflictIndex - (idx + 1)));
        }

        auto newLastIndex = idx + ents.size();
        commitTo(std::min(committed, newLastIndex));

        return newLastIndex;
    }

    void commitTo(Index idx)
    {
        // never decrease commit
        if (committed_ < idx) {
            if (lastIndex() < idx) {
                panic("commit index out of range");
            }
            committed_ = idx;
        }
    }

    void appliedTo(Index idx, size_t size)
    {
        if (idx == 0) {
            return;
        }

        if (committed_ < idx || idx < applied_) {
            panic("applied index out of range");
        }

        applied_ = idx;
        applying_ = std::max(applying_, applied_);
        if (applyingEntsSize_ > size) {
            applyingEntsSize_ -= size;
        } else {
            applyingEntsSize_ = 0;
        }
    }

    // hasNextUnstableEnts returns if there are any entries that are available to be
    // written to the local stable log and are not already in-progress.
    bool hasNextUnstableEntries() const { return !unstable_.hasNextEntries(); }

    // nextUnstableEnts returns all entries that are available to be written to the
    // local stable log and are not already in-progress.
    EntryList nextUnstableEntries() const { return unstable_.nextEntries(); }

    // nextUnstableSnapshot returns the snapshot, if present, that is available to
    // be applied to the local storage and is not already in-progress.
    SnapshotPtr nextUnstableSnapshot() { return unstable_.nextSnapshot(); }

    // hasNextOrInProgressSnapshot returns if there is pending snapshot waiting for
    // applying or in the process of being applied.
    bool hasNextUnstableSnapshot() const { return unstable_.nextSnapshot() != nullptr; }

    // nextCommittedEnts returns all the available entries for execution.
    // Entries can be committed even when the local raft instance has not durably
    // appended them to the local raft log yet. If allowUnstable is true, committed
    // entries from the unstable log may be returned; otherwise, only entries known
    // to reside locally on stable storage will be returned.
    EntryList nextCommittedEntries(bool allowUnstable)
    {
        if (applyingEntriePaused()) {
            // Entry application outstanding size limit reached.
            return {};
        }

        if (hasNextOrInProgressSnapshot()) {
            // See comment in hasNextCommittedEnts.
            return {};
        }

        //[lo, hi)
        auto lo = applying_ + 1;
        auto hi = maxAppliableIndex(allowUnstable) + 1;
        if (lo >= hi) {
            // Nothing to apply.
            return {};
        }

        auto maxSize = maxApplyingEntsSize_ - applyingEntsSize_;
        auto entries = slice(lo, hi, maxSize).unwrap();
        return entries;
    }

    // hasNextCommittedEnts returns if there is any available entries for execution.
    // This is a fast check without heavy raftLog.slice() in nextCommittedEnts().
    bool hasNextCommittedEntries(bool allowUnstable)
    {
        if (applyingEntriePaused()) {
            // Entry application outstanding size limit reached.
            return false;
        }
        if (hasNextOrInProgressSnapshot()) {
            // If we have a snapshot to apply, don't also return any committed
            // entries. Doing so raises questions about what should be applied
            // first.
            return false;
        }
        return applying_ < maxAppliableIndex(allowUnstable);
    }

    // hasNextOrInProgressUnstableEnts returns if there are any entries that are
    // available to be written to the local stable log or in the process of being
    // written to the local stable log.
    bool hasNextOrInProgressUnstableEnts() const { return !unstable_.entries().empty(); }

    // hasNextOrInProgressSnapshot returns if there is pending snapshot waiting for
    // applying or in the process of being applied.
    bool hasNextOrInProgressSnapshot() { return unstable_.snapshot() != nullptr; }

    inline void stableEntries(Index i, Term t) { unstable_.stableEntries(i, t); }

    inline void stableSnapshot(Index i) { unstable_.stableSnapshot(i); }

    // acceptUnstable indicates that the application has started persisting the
    // unstable entries in storage, and that the current unstable entries are thus
    // to be marked as being in-progress, to avoid returning them with future calls
    // to Ready().
    void acceptUnstable() { return unstable_.acceptInprogress(); }

    void acceptApplying(Index i, size_t size, bool allowUnstable)
    {
        if (committed_ < i) {
            panic("applying index out of range");
        }
        applying_ = i;
        applyingEntsSize_ += size;
    }

    // append returns last index of log.
    Index append(EntrySlice ents)
    {
        if (ents.empty()) {
            return lastIndex();
        }

        auto after = ents[0].index() - 1;
        if (after < committed_) {
            panic("append after out of range");
        }

        unstable_.appendEntries(ents);
        return lastIndex();
    }

    Result<EntryList> entries(Index idx, uint64_t maxSize = std::numeric_limits<uint64_t>::max()) const
    {
        auto lastIdx = lastIndex();
        if (idx > lastIdx) {
            return EntryList{};
        }
        return slice(idx, lastIdx + 1, maxSize);
    }

    void restore(std::shared_ptr<pb::Snapshot> s)
    {
        committed_ = s->metadata().index();
        unstable_.restore(s);
    }

    Result<SnapshotPtr> snapshot() const
    {
        if (unstable_.snapshot()) {
            return unstable_.snapshot();
        }
        return storage_.snapshot();
    }

    // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
    // by comparing the index and term of the last entries in the existing logs.
    // If the logs have last entries with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger lastIndex is more up-to-date. If the logs are
    // the same, the given log is up-to-date.
    bool isUpToDate(Index lastIdx, Term term) const
    {
        return term > lastTerm() || (term == lastTerm() && lastIdx >= lastIndex());
    }

    // scan visits all log entries in the [lo, hi) range, returning them via the
    // given callback. The callback can be invoked multiple times, with consecutive
    // sub-ranges of the requested range. Returns up to pageSize bytes worth of
    // entries at a time. May return more if a single entry size exceeds the limit.
    //
    // The entries in [lo, hi) must exist, otherwise scan() eventually returns an
    // error (possibly after passing some entries through the callback).
    //
    // If the callback returns an error, scan terminates and returns this error
    // immediately. This can be used to stop the scan early ("break" the loop).
    Result<> scan(Index lo, Index hi, uint64_t pageSize, std::function<Result<>(EntryList&)> f)
    {
        for (; lo < hi;) {
            auto entries = slice(lo, hi, pageSize);
            if (entries.has_error()) {
                return entries.error();
            } else if (entries->empty()) {
                return ErrLogScanEmpty;
            }
            auto res = f(*entries);
            if (res.has_error()) {
                return res.error();
            }
            lo += entries->size();
        }
        return {};
    }

    bool maybeCommit(Index maxIndex, Term t)
    {
        // NB: term should never be 0 on a commit because the leader campaigns at
        // least at term 1. But if it is 0 for some reason, we don't want to consider
        // this a term match in case zeroTermOnOutOfBounds returns 0.
        if (maxIndex > committed_ && t != 0 && term(maxIndex).value_or(0) == t) {
            commitTo(maxIndex);
            return true;
        }
        return false;
    }

private:
    // findConflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The index of the given entries MUST be continuously increasing.
    Index findConflict(EntrySlice ents)
    {
        for (auto& e : ents) {
            if (!matchTerm(e.index(), e.term())) {
                // if (e.index() <= lastIndex()) {
                // }
                return e.index();
            }
        }
        return 0;
    }

    // maxAppliableIndex returns the maximum committed index that can be applied.
    // If allowUnstable is true, committed entries from the unstable log can be
    // applied; otherwise, only entries known to reside locally on stable storage
    // can be applied.
    Index maxAppliableIndex(bool allowUnstable)
    {
        auto hi = committed_;
        if (!allowUnstable) {
            hi = std::min(hi, unstable_.offset() - 1);
        }
        return hi;
    }

    // allEntries returns all entries in the log.
    EntryList allEntries() const
    {
        auto ents = entries(firstIndex());
        if (ents) {
            return *ents;
        }

        if (ents.error() == ErrCompacted) { // try again if there was a racing compaction
            return allEntries();
        }
        // TODO (xiangli): handle error?
        panic("unknown error: %d", int(ents.error()));
    }

    // slice returns a slice of log entries of range [lo, hi).
    Result<EntryList> slice(Index lo, Index hi, uint64_t maxSize) const
    {
        if (lo > hi) {
            panic("invalid slice {} > {}", lo, hi);
        }

        if (lo < firstIndex()) {
            return ErrCompacted;
        }

        if (hi > lastIndex() + 1) {
            panic("slice[{},{}) out of bound [{},{}]", lo, hi, firstIndex(), lastIndex());
        }

        if (lo == hi) {
            return EntryList{};
        }

        if (lo >= unstable_.offset()) {
            auto ents = unstable_.slice(lo, hi, maxSize);
            // NB: use the full slice expression to protect the unstable slice from
            // appends to the returned ents slice.
            return EntryList{ ents.begin(), ents.end() };
        }

        auto cut = std::min(hi, unstable_.offset());
        auto entries = storage_.entries(lo, cut, maxSize);

        if (entries.has_error()) {
            if (entries.error() == ErrCompacted) {
                return entries;
            }
            entries.unwrap();
        }

        if (hi <= unstable_.offset()) {
            return *entries;
        }

        // Fast path to check if ents has reached the size limitation. Either the
        // returned slice is shorter than requested (which means the next entry would
        // bring it over the limit), or a single entry reaches the limit.
        if (entries->size() < cut - lo) {
            return *entries;
        }
        // Slow path computes the actual total size, so that unstable entries are cut
        // optimally before being copied to ents slice.
        auto size = entsSize(*entries);
        if (size >= maxSize) {
            return *entries;
        }

        auto unstable = unstable_.slice(unstable_.offset(), hi, maxSize - size);
        // Total size of unstable may exceed maxSize-size only if len(unstable) == 1.
        // If this happens, ignore this extra entry.
        if (unstable.size() == 1 && size + entsSize(unstable) > maxSize) {
            return entries;
        }

        // Otherwise, total size of unstable does not exceed maxSize-size, so total
        // size of ents+unstable does not exceed maxSize. Simply concatenate them.
        entries->insert(entries->end(), unstable.begin(), unstable.end());
        return std::move(*entries);
    }

    bool applyingEntriePaused() const { return applyingEntsSize_ >= maxApplyingEntsSize_; }

public:
    // storage contains all stable entries since the last snapshot.
    T& storage_;

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    Unstable unstable_;

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    Index committed_;

    // applying is the highest log position that the application has
    // been instructed to apply to its state machine. Some of these
    // entries may be in the process of applying and have not yet
    // reached applied.
    // Use: The field is incremented when accepting a Ready struct.
    // Invariant: applied <= applying && applying <= committed
    Index applying_;

    // applied is the highest log position that the application has
    // successfully applied to its state machine.
    // Use: The field is incremented when advancing after the committed
    // entries in a Ready struct have been applied (either synchronously
    // or asynchronously).
    // Invariant: applied <= committed
    Index applied_;

    // maxApplyingEntsSize limits the outstanding byte size of the messages
    // returned from calls to nextCommittedEnts that have not been acknowledged
    // by a call to appliedTo.
    const size_t maxApplyingEntsSize_;
    // applyingEntsSize is the current outstanding byte size of the messages
    // returned from calls to nextCommittedEnts that have not been acknowledged
    // by a call to appliedTo.
    size_t applyingEntsSize_;
};

} // namespace raft