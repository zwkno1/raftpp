#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <confchange/confchange.h>
#include <quorum/joint.h>
#include <quorum/quorum.h>
#include <tracker/progress.h>
#include <utils.h>

#include <raftpb/raft.pb.h>

namespace raft {
namespace tracker {

// Config reflects the configuration tracked in a ProgressTracker.
class Config
{
public:
    Config()
      : autoLeave_(false)
    {
    }

    inline bool isJoint() const { return voters_.isJoint(); }

    inline Config clone() const
    {
        auto c = *this;
        c.autoLeave_ = false;
        return c;
    }

    quorum::JointConfig voters_;
    // AutoLeave is true if the configuration is joint and a transition to the
    // incoming configuration should be carried out automatically by Raft when
    // this is possible. If false, the configuration will be joint until the
    // application initiates the transition manually.
    bool autoLeave_;
    // Learners is a set of IDs corresponding to the learners active in the
    // current configuration.
    //
    // Invariant: Learners and Voters does not intersect, i.e. if a peer is in
    // either half of the joint config, it can't be a learner; if it is a
    // learner it can't be in either half of the joint config. This invariant
    // simplifies the implementation since it allows peers to have clarity about
    // its current role without taking into account joint consensus.
    std::set<uint64_t> learners_;
    // When we turn a voter into a learner during a joint consensus transition,
    // we cannot add the learner directly when entering the joint state. This is
    // because this would violate the invariant that the intersection of
    // voters and learners is empty. For example, assume a Voter is removed and
    // immediately re-added as a learner (or in other words, it is demoted):
    //
    // Initially, the configuration will be
    //
    //   voters:   {1 2 3}
    //   learners: {}
    //
    // and we want to demote 3. Entering the joint configuration, we naively get
    //
    //   voters:   {1 2} & {1 2 3}
    //   learners: {3}
    //
    // but this violates the invariant (3 is both voter and learner). Instead,
    // we get
    //
    //   voters:   {1 2} & {1 2 3}
    //   learners: {}
    //   next_learners: {3}
    //
    // Where 3 is now still purely a voter, but we are remembering the intention
    // to make it a learner upon transitioning into the final configuration:
    //
    //   voters:   {1 2}
    //   learners: {3}
    //   next_learners: {}
    //
    // Note that next_learners is not used while adding a learner that is not
    // also a voter in the joint config. In this case, the learner is added
    // right away when entering the joint configuration, so that it is caught up
    // as soon as possible.
    std::set<uint64_t> learnersNext_;
};

class MatchAckIndexer
{
public:
    MatchAckIndexer(const std::map<uint64_t, ProgressPtr>& p)
      : progress_(p)
    {
    }

    // AckedIndex implements IndexLookuper.
    std::optional<Index> ackedIndex(uint64_t id) const
    {
        auto iter = progress_.find(id);
        if (iter == progress_.end()) {
            return {};
        }
        return iter->second->match();
    }

private:
    const std::map<uint64_t, ProgressPtr>& progress_;
};

struct VoteDetails
{
    size_t granted_ = 0;
    size_t rejected_ = 0;
    quorum::VoteResult result_ = quorum::VoteLost;
};

// ProgressTracker tracks the currently active configuration and the information
// known about the nodes and learners in it. In particular, it tracks the match
// index for each peer which in turn allows reasoning about the committed index.
class ProgressTracker
{
public:
    ProgressTracker(size_t maxInflight, size_t maxBytes)
      : maxInflight_(maxInflight)
      , maxInflightBytes_(maxBytes)
    {
    }

    ProgressTracker(size_t maxInflight, size_t maxBytes, Config config, ProgressMap progress)
      : maxInflight_(maxInflight)
      , maxInflightBytes_(maxBytes)
      , config_(std::move(config))
      , progress_(std::move(progress))
    {
    }

    // ConfState returns a ConfState representing the active configuration.
    pb::ConfState confState()
    {
        pb::ConfState cs;

        auto copy = [](auto dst, const auto& src) {
            dst->Reserve(src.size());
            dst->Assign(src.begin(), src.end());
        };

        copy(cs.mutable_voters(), config_.voters_.incoming().ids());
        copy(cs.mutable_voters_outgoing(), config_.voters_.outgoing().ids());

        copy(cs.mutable_learners(), config_.learners_);
        copy(cs.mutable_learners_next(), config_.learnersNext_);

        cs.set_auto_leave(config_.autoLeave_);

        return cs;
    }

    const Config& config() const { return config_; }

    ProgressPtr getProgress(uint64_t id)
    {
        auto iter = progress_.find(id);
        if (iter == progress_.end()) {
            return nullptr;
        }
        return iter->second;
    }

    bool contains(uint64_t id) const { return progress_.contains(id); }

    const std::unordered_map<uint64_t, bool>& votes() const { return votes_; }

    // IsSingleton returns true if (and only if) there is only one voting member
    // (i.e. the leader) in the current configuration.
    bool isSingleton() const
    {
        return (config_.voters_.incoming().ids().size() == 1) && config_.voters_.outgoing().ids().empty();
    }

    // Committed returns the largest log index known to be committed based on what
    // the voting members of the group have acknowledged.
    Index committedIndex() const
    {
        MatchAckIndexer l{ progress_ };
        return config_.voters_.committedIndex(l);
    }

    // Visit invokes the supplied closure for all tracked progresses in stable order.
    void visit(std::function<void(uint64_t, Progress&)> f)
    {
        for (auto& [id, pr] : progress_) {
            f(id, *pr);
        }
    }

    // QuorumActive returns true if the quorum is active from the view of the local
    // raft state machine. Otherwise, it returns false.
    bool quorumActive()
    {
        // votes := map[uint64]bool{}
        std::unordered_map<uint64_t, bool> votes;
        for (auto& [id, pr] : progress_) {
            if (config_.learners_.contains(id)) {
                continue;
            }
            votes[id] = pr->recentActive();
        }

        return config_.voters_.voteResult(votes) == quorum::VoteWon;
    }

    // ResetVotes prepares for a new round of vote counting via recordVote.
    void resetVotes() { votes_.clear(); }

    // RecordVote records that the node with the given id voted for this Raft
    // instance if v == true (and declined it otherwise).
    void recordVote(uint64_t id, bool v) { votes_.try_emplace(id, v); }

    // TallyVotes returns the number of granted and rejected Votes, and whether the
    // election outcome is known.
    VoteDetails tallyVotes()
    {
        // Make sure to populate granted/rejected correctly even if the Votes slice
        // contains members no longer part of the configuration. This doesn't really
        // matter in the way the numbers are used (they're informational), but might
        // as well get it right.

        VoteDetails details;

        for (auto& [id, pr] : progress_) {
            if (config_.learners_.contains(id)) {
                continue;
            }

            auto iter = votes_.find(id);
            if (iter == votes_.end()) {
                continue;
            }
            if (iter->second) {
                details.granted_++;
            } else {
                details.rejected_++;
            }
        }

        details.result_ = config_.voters_.voteResult(votes_);
        return details;
    }

    void update(Config config, ProgressMap progress)
    {
        config_ = std::move(config);
        progress_ = std::move(progress);
    }

    ProgressPtr create(Index lastIndex, bool recentActive) const
    {
        return std::make_shared<tracker::Progress>(lastIndex, maxInflight_, maxInflightBytes_, recentActive);
    }

    ProgressMap progress() { return progress_; };

    bool isLearner(uint64_t id) { return config_.learners_.contains(id); }

private:
    size_t maxInflight_;

    size_t maxInflightBytes_;

    Config config_;

    ProgressMap progress_;

    std::unordered_map<uint64_t, bool> votes_;
};

} // namespace tracker
} // namespace raft