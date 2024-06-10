#pragma once

#include <format>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include <raftpp/detail/message.h>

namespace raft {
namespace quorum {

// VoteResult indicates the outcome of a vote.
enum VoteResult : uint32_t
{
    // VotePending indicates that the decision of the vote depends on future
    // votes, i.e. neither "yes" or "no" has reached quorum yet.
    VotePending = 1,
    // VoteLost indicates that the quorum has voted "no".
    VoteLost,
    // VoteWon indicates that the quorum has voted "yes".
    VoteWon
};

// AckedIndexer allows looking up a commit index for a given ID of a voter
// from a corresponding MajorityConfig.
template <typename T>
concept AckedIndexer = std::same_as<std::invoke_result_t<T, NodeId>, std::optional<Index>>;

// Votes concept
template <typename T>
concept Votes = std::same_as<std::invoke_result_t<T, NodeId>, VoteResult>;

class MapAckIndexer
{
public:
    std::optional<Index> operator()(NodeId id) const
    {
        auto iter = idxs_.find(id);
        if (iter != idxs_.end()) {
            return iter->second;
        }
        return {};
    }

    void add(NodeId id, Index idx) { idxs_[id] = idx; }

private:
    std::unordered_map<NodeId, Index> idxs_;
};

class MajorityConfig
{
public:
    MajorityConfig() = default;

    // CommittedIndex computes the committed index from those supplied via the
    // provided AckedIndexer (for the active config).
    template <AckedIndexer T>
    Index committedIndex(const T& l) const
    {
        if (ids_.empty()) {
            // This plays well with joint quorums which, when one half is the zero
            // MajorityConfig, should behave like the other half.
            return IndexMax;
        }

        std::array<Index, 16> arr;
        std::vector<Index> vec;

        Index* idxs = arr.data();
        size_t n = 0;

        if (ids_.size() > arr.size()) {
            vec.resize(ids_.size());
            idxs = vec.data();
        }

        for (auto id : ids_) {
            auto idx = l(id);
            if (idx) {
                idxs[n++] = *idx;
            }
        }

        auto q = ids_.size() / 2;
        if (n <= q) {
            return 0;
        }

        // The smallest index into the array for which the value is acked by a
        // quorum. In other words, from the end of the slice, move n/2+1 to the
        // left (accounting for zero-indexing).
        std::nth_element(idxs, idxs + q, idxs + n, std::greater<>{});
        return idxs[q];
    }

    // VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    // a result indicating whether the vote is pending (i.e. neither a quorum of
    // yes/no has been reached), won (a quorum of yes has been reached), or lost (a
    // quorum of no has been reached).
    template <Votes T>
    VoteResult voteResult(const T& votes) const
    {
        if (ids_.empty()) {
            // By convention, the elections on an empty config win. This comes in
            // handy with joint quorums because it'll make a half-populated joint
            // quorum behave like a majority quorum.
            return VoteWon;
        }

        size_t voted = 0;
        size_t missing = 0;

        for (auto id : ids_) {
            auto result = votes(id);
            if (result == VotePending) {
                ++missing;
            } else if (result == VoteWon) {
                ++voted;
            }
        }

        auto q = ids_.size() / 2 + 1;

        if (voted >= q) {
            return VoteWon;
        }

        if (voted + missing >= q) {
            return VotePending;
        }

        return VoteLost;
    }

    inline void add(NodeId id) { ids_.insert(id); }

    inline void remove(NodeId id) { ids_.erase(id); }

    inline void clear() { ids_.clear(); }

    inline const std::unordered_set<NodeId>& ids() const { return ids_; }

    inline bool contains(NodeId id) const { return ids_.contains(id); }

    inline bool empty() const { return ids_.empty(); }

private:
    std::unordered_set<NodeId> ids_;
};

class JointConfig
{
public:
    // CommittedIndex returns the largest committed index for the given joint
    // quorum. An index is jointly committed if it is committed in both constituent
    // majorities.
    template <AckedIndexer T>
    Index committedIndex(const T& l) const
    {
        auto idx0 = incoming_.committedIndex(l);
        auto idx1 = outgoing_.committedIndex(l);
        return std::min(idx0, idx1);
    }

    // VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    // a result indicating whether the vote is pending, lost, or won. A joint quorum
    // requires both majority quorums to vote in favor.
    template <Votes T>
    VoteResult voteResult(const T& votes) const
    {
        auto r1 = incoming_.voteResult(votes);
        auto r2 = outgoing_.voteResult(votes);

        if (r1 == r2) {
            // If they agree, return the agreed state.
            return r1;
        }

        if (r1 == VoteLost || r2 == VoteLost) {
            // If either config has lost, loss is the only possible outcome.
            return VoteLost;
        }
        // One side won, the other one is pending, so the whole outcome is.
        return VotePending;
    }

    inline MajorityConfig& incoming() { return incoming_; }

    inline const MajorityConfig& incoming() const { return incoming_; }

    inline MajorityConfig& outgoing() { return outgoing_; }

    inline const MajorityConfig& outgoing() const { return outgoing_; }

    inline bool isJoint() const { return !outgoing_.empty(); }

    inline bool contains(NodeId id) const { return incoming_.contains(id) || outgoing_.contains(id); }

    inline bool empty() const { return incoming_.empty() && outgoing_.empty(); }

private:
    // IDs returns a newly initialized map representing the set of voters present
    // in the joint configuration.
    // std::set<NodeId> ids() const
    //{
    //    std::set<NodeId> ids;
    //    std::set_union(incoming_.ids().begin(), incoming_.ids().end(), outgoing_.ids().begin(), outgoing_.ids().end(),
    //                   std::inserter(ids, ids.begin()));
    //    return ids;
    //}

    MajorityConfig incoming_;
    MajorityConfig outgoing_;
};

} // namespace quorum
} // namespace raft

template <typename CharT>
struct std::formatter<raft::quorum::MajorityConfig, CharT> : std::formatter<string_view, CharT>
{
    template <class FormatContext>
    auto format(const raft::quorum::MajorityConfig& m, FormatContext& ctx) const
    {
        return std::format_to(ctx.out(), "{}", m.ids());
    }
};

template <typename CharT>
struct std::formatter<raft::quorum::JointConfig, CharT> : std::formatter<string_view, CharT>
{
    template <class FormatContext>
    auto format(const raft::quorum::JointConfig& joint, FormatContext& ctx) const
    {

        if (joint.outgoing().empty()) {
            return std::format_to(ctx.out(), "{}", joint.incoming());
        }
        return std::format_to(ctx.out(), "[{},{}]", joint.incoming(), joint.outgoing());
    }
};
