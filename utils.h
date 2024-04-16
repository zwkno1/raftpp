#pragma once

#include <cstddef>
#include <cstdlib>
#include <exception>
#include <limits>
#include <memory>
#include <numeric>
#include <ranges>
#include <span>
#include <type_traits>

#include <error.h>
#include <result.h>
#include <spdlog/logger.h>

#include <raftpb/raft.pb.h>

namespace raft {

// Index is a Raft log position.
using Index = uint64_t;
using Term = uint64_t;

struct IndexTerm
{
    Index index;
    Term term;
};

constexpr Index IndexMax = std::numeric_limits<Index>::max();

inline std::string to_string(Index i)
{
    if (i == IndexMax) {
        return "∞";
    }
    return std::to_string(static_cast<uint64_t>(i));
}


using Logger = ::spdlog::logger; 

using EntryList = std::vector<pb::Entry>;

template <typename Iter>
class EntryView
{
public:
    static constexpr size_t npos = std::numeric_limits<size_t>::max();

    EntryView() = default;

    template <typename T>
    EntryView(T& t)
      : beg_(std::begin(t))
      , end_(std::end(t))
    {
    }

    EntryView(Iter beg, Iter end)
      : beg_(beg)
      , end_(end)
    {
    }

    Iter begin() const { return beg_; }
    Iter end() const { return end_; }
    size_t size() const { return end_ - beg_; }

    pb::Entry& operator[](size_t idx) { return *(beg_ + idx); }

    EntryView<Iter> sub(size_t pos = 0, size_t n = npos)
    {
        auto beg = beg_;
        beg += pos;
        auto end = end_;
        if (n != npos) {
            end = beg_;
            end += pos + n;
        }
        return { beg, end };
    }

    bool empty() const { return beg_ == end_; }

private:
    Iter beg_;
    Iter end_;
};

using EntrySlice = EntryView<google::protobuf::RepeatedPtrField<pb::Entry>::iterator>;
using ConstEntrySlice = EntryView<google::protobuf::RepeatedPtrField<pb::Entry>::const_iterator>;

using SnapshotPtr = std::shared_ptr<pb::Snapshot>;

inline bool operator==(const pb::HardState& l, const pb::HardState& r)
{
    return (l.term() == r.term()) && (l.vote() == r.vote()) && (l.commit() == r.commit());
}

inline bool isEmptyHardState(const pb::HardState& s)
{
    const static pb::HardState empty;
    return s == empty;
}

inline void assertConfStatesEquivalent(const pb::ConfState& l, const pb::ConfState& r)
{
}

inline bool isEmptySnap(const pb::Snapshot& sp)
{
    return sp.metadata().index() == 0;
}

template <typename T>
size_t payloadsSize(const T& x)
{
    if constexpr (std::is_same_v<pb::Entry, T>) {
        return x.data().size();
    } else {
        return std::accumulate(x.begin(), x.end(), size_t{ 0 },
                               [](size_t sz, auto& e) { return sz + payloadsSize(e); });
    }
}

template <typename T>
size_t entsSize(const T& ents)
{
    uint64_t size = 0;
    for (auto& e : ents) {
        size += e.ByteSizeLong();
    }
    return size;
}

inline pb::Message confChangeToMsg(const pb::ConfChange& c = pb::ConfChange{})
{
    pb::Message msg;
    msg.set_type(pb::MsgPropose);
    auto ent = msg.add_entries();
    ent->set_type(pb::EntryConfChange);
    ent->set_data(c.SerializeAsString());
    return msg;
}

pb::MessageType voteRespMsgType(pb::MessageType t)
{
    switch (t) {
    case pb::MsgRequestVote:
        return pb::MsgRequestVoteResponse;
    case pb::MsgRequestPreVote:
        return pb::MsgRequestPreVoteResponse;
    default:
        panic("not a vote message: {}", pb::MessageType_Name(t));
    }
}

inline bool isResponseMsg(pb::MessageType t)
{
    switch (t) {
    case pb::MsgAppendResponse:
    case pb::MsgRequestVoteResponse:
    case pb::MsgHeartbeatResponse:
    case pb::MsgReadIndexResponse:
    case pb::MsgRequestPreVoteResponse:
        return true;
    default:
        return false;
    }
}

// InvalidId is a placeholder node ID used when there is no leader.
constexpr uint64_t InvalidId = 0;

// limitSize returns the longest prefix of the given entries slice, such that
// its total byte size does not exceed maxSize. Always returns a non-empty slice
// if the input is non-empty, so, as an exception, if the size of the first
// entry exceeds maxSize, a non-empty slice with just this entry is returned.
template <typename T>
static T limitSize(const T& entries, uint64_t maxSize)
{
    if (entries.empty()) {
        return entries;
    }

    auto size = 0;
    auto iter = std::find_if(entries.begin(), entries.end(), [&](const pb::Entry& e) {
        size += e.ByteSizeLong();
        return (size > maxSize);
    });

    if (iter == entries.begin()) {
        ++iter;
    }

    return { entries.begin(), iter };
}

} // namespace raft