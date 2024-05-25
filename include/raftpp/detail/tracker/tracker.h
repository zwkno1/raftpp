#pragma once

#include <optional>
#include <unordered_map>

#include <raftpp/detail/message.h>
#include <raftpp/detail/quorum.h>
#include <raftpp/detail/tracker/progress.h>

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
    std::set<NodeId> learners_;
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
    std::set<NodeId> learnersNext_;
};

template <typename T>
concept ProgressVisitor = std::invocable<T, NodeId, Progress&>;

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

    // ConfState returns a ConfState representing the active configuration.
    ConfState confState() const
    {
        ConfState cs;

        auto copy = [](auto& dst, const auto& src) {
            dst.reserve(src.size());
            dst = { src.begin(), src.end() };
        };

        copy(cs.voters, config_.voters_.incoming().ids());
        copy(cs.votersOutgoing, config_.voters_.outgoing().ids());

        copy(cs.learners, config_.learners_);
        copy(cs.learnersNext, config_.learnersNext_);

        cs.autoLeave = config_.autoLeave_;

        return cs;
    }

    const Config& config() const { return config_; }

    ProgressPtr getProgress(NodeId id)
    {
        auto iter = progress_.find(id);
        if (iter == progress_.end()) {
            return nullptr;
        }
        return iter->second;
    }

    bool contains(NodeId id) const { return progress_.contains(id); }

    const std::unordered_map<NodeId, bool>& votes() const { return votes_; }

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
        return config_.voters_.committedIndex([&](NodeId id) -> std::optional<Index> {
            auto iter = progress_.find(id);
            if (iter != progress_.end()) {
                return iter->second->match();
            }
            return {};
        });
    }

    // Visit invokes the supplied closure for all tracked progresses in stable order.
    template <ProgressVisitor T>
    void visit(T&& func)
    {
        for (auto& [id, pr] : progress_) {
            func(id, *pr);
        }
    }

    // QuorumActive returns true if the quorum is active from the view of the local
    // raft state machine. Otherwise, it returns false.
    bool quorumActive()
    {
        return config_.voters_.voteResult([&](NodeId id) {
            auto iter = progress_.find(id);
            if (iter == progress_.end()) {
                return quorum::VotePending;
            }
            return iter->second->recentActive() ? quorum::VoteWon : quorum::VoteLost;
        }) == quorum::VoteWon;
    }

    // ResetVotes prepares for a new round of vote counting via recordVote.
    void resetVotes() { votes_.clear(); }

    // RecordVote records that the node with the given id voted for this Raft
    // instance if v == true (and declined it otherwise).
    void recordVote(NodeId id, bool v) { votes_.try_emplace(id, v); }

    // voteResult returns the election outcome.
    quorum::VoteResult voteResult()
    {
        return config_.voters_.voteResult([&](NodeId id) {
            auto iter = votes_.find(id);
            if (iter == votes_.end()) {
                return quorum::VotePending;
            }
            return iter->second ? quorum::VoteWon : quorum::VoteLost;
        });
    }

    void reset(Config config, ProgressMap progress)
    {
        config_ = std::move(config);
        progress_ = std::move(progress);
    }

    ProgressPtr create(Index lastIndex, bool recentActive) const
    {
        return std::make_shared<tracker::Progress>(lastIndex, maxInflight_, maxInflightBytes_, recentActive);
    }

    ProgressMap progress() const { return progress_; };

    bool isLearner(NodeId id) const { return config_.learners_.contains(id); }

private:
    size_t maxInflight_;

    size_t maxInflightBytes_;

    Config config_;

    ProgressMap progress_;

    std::unordered_map<NodeId, bool> votes_;
};

} // namespace tracker
} // namespace raft
