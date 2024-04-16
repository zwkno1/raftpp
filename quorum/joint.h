#pragma once

#include <array>
#include <set>
#include <string>
#include <unordered_map>

#include <quorum/majority.h>

namespace raft {
namespace quorum {

// JointConfig is a configuration of two groups of (possibly overlapping)
// majority configurations. Decisions require the support of both majorities.
class JointConfig
{
public:
    bool empty() const { return incoming_.empty() && outgoing_.empty(); }

    // Describe returns a (multi-line) representation of the commit indexes for the
    // given lookuper.

    template <AckedIndexer T>
    std::string describe(const T& l) const
    {
        return MajorityConfig{ ids() }.describe(l);
    }

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
    VoteResult voteResult(const std::unordered_map<uint64_t, bool>& votes) const
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

    std::string str() const
    {
        if (!outgoing_.ids().empty()) {
            return incoming_.str() + "&&" + outgoing_.str();
        }
        return incoming_.str();
    }

    MajorityConfig& incoming() { return incoming_; }

    const MajorityConfig& incoming() const { return incoming_; }

    MajorityConfig& outgoing() { return outgoing_; }

    const MajorityConfig& outgoing() const { return outgoing_; }

    bool isJoint() const { return !outgoing_.empty(); }

    inline bool contains(uint64_t id) const { return incoming_.contains(id) || outgoing_.contains(id); }

private:
    // IDs returns a newly initialized map representing the set of voters present
    // in the joint configuration.
    std::set<uint64_t> ids() const
    {
        std::set<uint64_t> ids;
        for (auto id : incoming_.ids()) {
            ids.insert(id);
        }
        for (auto id : outgoing_.ids()) {
            ids.insert(id);
        }
        return ids;
    }

    MajorityConfig incoming_;
    MajorityConfig outgoing_;
};

} // namespace quorum
} // namespace raft