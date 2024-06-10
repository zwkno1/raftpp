#pragma once

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <raftpp/detail/error.h>
#include <raftpp/detail/message.h>
#include <raftpp/detail/result.h>
#include <raftpp/detail/tracker/tracker.h>

namespace raft {

namespace confchange {

inline bool leaveJoint(const ConfChange& cc)
{
    return cc.changes.empty() && cc.context.empty() && (cc.transition == ConfChangeTransition::Auto);
}

std::pair<bool, bool> enterJoint(const ConfChange& cc)
{
    if ((cc.transition != ConfChangeTransition::Auto) || (cc.changes.size() > 1)) {
        if ((cc.transition != ConfChangeTransition::Auto) && (cc.transition != ConfChangeTransition::Implicit)) {
            panic("unknown transition");
        }
        return { true, true };
    }
    return { false, false };
}

struct ChangerResult
{
    tracker::Config config_;
    tracker::ProgressMap progress_;
};

// Changer facilitates configuration changes. It exposes methods to handle
// simple and joint consensus while performing the proper validation that allows
// refusing invalid configuration changes before they affect the active
// configuration.
struct Changer
{
    Changer(tracker::ProgressTracker& tracker, Index lastIndex)
      : tracker_(tracker)
      , lastIndex_(lastIndex)
      , config_(tracker.config())
      , prs_(tracker.progress())
    {
    }

    tracker::ProgressTracker& tracker_;
    Index lastIndex_;

    tracker::Config config_;
    tracker::ProgressMap prs_;

    // EnterJoint verifies that the outgoing (=right) majority config of the joint
    // config is empty and initializes it with a copy of the incoming (=left)
    // majority config. That is, it transitions from
    //
    //	(1 2 3)&&()
    //
    // to
    //
    //	(1 2 3)&&(1 2 3).
    //
    // The supplied changes are then applied to the incoming majority config,
    // resulting in a joint configuration that in terms of the Raft thesis[1]
    // (Section 4.3) corresponds to `C_{new,old}`.
    //
    // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    Result<ChangerResult, Error> enterJoint(bool autoLeave, std::span<const ConfChangeItem> css)
    {
        auto res = checkAndCopy();
        if (res.has_error()) {
            return res;
        }
        auto& cfg = res->config_;
        auto& progress = res->progress_;

        if (cfg.isJoint()) {
            return Error::fmt("config is already joint");
        }

        if (cfg.voters_.incoming().empty()) {
            // We allow adding nodes to an empty config for convenience (testing and
            // bootstrap), but you can't enter a joint state.
            return Error::fmt("can't make a zero-voter config joint");
        }

        // Copy incoming to outgoing.
        cfg.voters_.outgoing() = cfg.voters_.incoming();

        auto ok = apply(cfg, progress, css);
        if (ok.has_error()) {
            return ok.error();
        }

        cfg.autoLeave_ = autoLeave;
        ok = checkInvariants(cfg, progress);
        if (ok.has_error()) {
            return ok.error();
        }

        return res;
    }

    // Simple carries out a series of configuration changes that (in aggregate)
    // mutates the incoming majority config Voters[0] by at most one. This method
    // will return an error if that is not the case, if the resulting quorum is
    // zero, or if the configuration is in a joint state (i.e. if there is an
    // outgoing configuration).
    Result<ChangerResult, Error> simple(std::span<const ConfChangeItem> ccs)
    {
        auto res = checkAndCopy();
        if (res.has_error()) {
            return res;
        }
        auto& cfg = res->config_;
        auto& trk = res->progress_;

        if (cfg.isJoint()) {
            return Error::fmt("can't apply simple config change in joint config");
        }

        auto ok = apply(cfg, trk, ccs);
        if (ok.has_error()) {
            return ok.error();
        }

        if (symdiff(config_.voters_.incoming(), cfg.voters_.incoming()) > 1) {
            return Error::fmt("more than one voter changed without entering joint config");
        }

        ok = checkInvariants(cfg, trk);
        if (ok.has_error()) {
            return ok.error();
        }
        return res;
    }

    // LeaveJoint transitions out of a joint configuration. It is an error to call
    // this method if the configuration is not joint, i.e. if the outgoing majority
    // config Voters[1] is empty.
    //
    // The outgoing majority config of the joint configuration will be removed,
    // that is, the incoming config is promoted as the sole decision maker. In the
    // notation of the Raft thesis[1] (Section 4.3), this method transitions from
    // `C_{new,old}` into `C_new`.
    //
    // At the same time, any staged learners (LearnersNext) the addition of which
    // was held back by an overlapping voter in the former outgoing config will be
    // inserted into Learners.
    //
    // [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    Result<ChangerResult, Error> leaveJoint()
    {
        auto res = checkAndCopy();
        if (res.has_error()) {
            return res;
        }
        auto& cfg = res->config_;
        auto& prs = res->progress_;

        if (!cfg.isJoint()) {
            return Error::fmt("can't leave a non-joint config");
        }

        for (auto id : cfg.learnersNext_) {
            cfg.learners_.insert(id);
        }
        cfg.learnersNext_.clear();

        for (auto id : cfg.voters_.outgoing().ids()) {
            bool isVoter = cfg.voters_.incoming().ids().contains(id);
            bool isLearner = cfg.learners_.contains(id);

            if (!isVoter && !isLearner) {
                prs.erase(id);
            }
        }
        cfg.voters_.outgoing().clear();
        cfg.autoLeave_ = false;

        auto err = checkInvariants(cfg, prs);
        if (err.has_error()) {
            return err.error();
        }
        return res;
    }

private:
    // symdiff returns the count of the symmetric difference between the sets of
    // uint64s, i.e. len( (l - r) \union (r - l)).
    size_t symdiff(const quorum::MajorityConfig& l, const quorum::MajorityConfig& r)
    {
        auto diff = [](auto& x, auto& y) {
            size_t n = 0;
            for (auto& i : x.ids()) {
                if (!y.ids().contains(i)) {
                    ++n;
                }
            }
            return n;
        };

        return diff(l, r) + diff(r, l);
    }

    // checkAndCopy copies the tracker's config and progress map (deeply enough for
    // the purposes of the Changer) and returns those copies. It returns an error
    // if checkInvariants does.
    Result<ChangerResult, Error> checkAndCopy()
    {
        auto cfg = config_.clone();
        tracker::ProgressMap prs = prs_;

        auto res = checkInvariants(cfg, prs);
        if (res.has_error()) {
            return res.error();
        }
        return ChangerResult{ cfg, prs };
    }

    // checkInvariants makes sure that the config and progress are compatible with
    // each other. This is used to check both what the Changer is initialized with,
    // as well as what it returns.
    Result<void, Error> checkInvariants(const tracker::Config& cfg, const tracker::ProgressMap& prs)
    {
        // NB: intentionally allow the empty config. In production we'll never see a
        // non-empty config (we prevent it from being created) but we will need to
        // be able to *create* an initial config, for example during bootstrap (or
        // during tests). Instead of having to hand-code this, we allow
        // transitioning from an empty config into any other legal and non-empty
        // config.
        for (auto id : cfg.voters_.incoming().ids()) {
            if (!prs.contains(id)) {
                return Error::fmt("no progress for %d", id);
            }
        }
        for (auto id : cfg.voters_.outgoing().ids()) {
            if (!prs.contains(id)) {
                return Error::fmt("no progress for %d", id);
            }
        }

        // Conversely Learners and Voters doesn't intersect at all.
        for (auto id : cfg.learners_) {
            if (!prs.contains(id)) {
                return Error::fmt("no progress for %d", id);
            }
            if (cfg.voters_.outgoing().contains(id)) {
                return Error::fmt("%d is in Learners and Voters[1]", id);
            }
            if (cfg.voters_.incoming().contains(id)) {
                return Error::fmt("%d is in Learners and Voters[0]", id);
            }
        }

        // Any staged learner was staged because it could not be directly added due
        // to a conflicting voter in the outgoing config.
        for (auto id : cfg.learnersNext_) {
            if (!prs.contains(id)) {
                return Error::fmt("no progress for %d", id);
            }

            if (!cfg.voters_.outgoing().contains(id)) {
                return Error::fmt("%d is in LearnersNext, but not Voters[1]", id);
            }

            // if (cfg.learners_.contains(id)) {
            //     return Error::fmt("%d is in LearnersNext, but is already marked as learner", id);
            // }
        }

        if (!cfg.isJoint()) {
            if (!cfg.learnersNext_.empty()) {
                return Error::fmt("cfg.LearnersNext must be nil when not joint");
            }

            if (cfg.autoLeave_) {
                return Error::fmt("AutoLeave must be false when not joint");
            }
        }
        return {};
    }

    // apply a change to the configuration. By convention, changes to voters are
    // always made to the incoming majority config Voters[0]. Voters[1] is either
    // empty or preserves the outgoing majority configuration while in a joint state.
    Result<void, Error> apply(tracker::Config& cfg, tracker::ProgressMap& trk, std::span<const ConfChangeItem> ccs)
    {
        for (auto& cc : ccs) {
            if (cc.nodeId == 0) {
                // etcd replaces the NodeID with zero if it decides (downstream of
                // raft) to not apply a change, so we have to have explicit code
                // here to ignore these.
                continue;
            }

            switch (cc.type) {
            case AddNode:
                makeVoter(cfg, trk, cc.nodeId);
                break;
            case AddLearnerNode:
                makeLearner(cfg, trk, cc.nodeId);
                break;
            case RemoveNode:
                remove(cfg, trk, cc.nodeId);
                break;
            default:
                return Error::fmt("unexpected conf type"); //, ConfChangeType_Name(cc.type));
            }
        }

        if (cfg.voters_.incoming().empty()) {
            return Error::fmt("removed all voters");
        }
        return {};
    }

    // makeVoter adds or promotes the given ID to be a voter in the incoming
    // majority config.
    void makeVoter(tracker::Config& cfg, tracker::ProgressMap& trk, NodeId id)
    {
        auto p = initProgress(cfg, trk, id, false);

        cfg.learners_.erase(id);
        cfg.learnersNext_.erase(id);
        cfg.voters_.incoming().add(id);
    }

    // initProgress initializes a new progress for the given node or learner.
    tracker::ProgressPtr initProgress(tracker::Config& cfg, tracker::ProgressMap& progresses, NodeId id, bool isLearner)
    {
        auto& p = progresses[id];
        if (p) {
            return p;
        }

        if (!isLearner) {
            cfg.voters_.incoming().add(id);
        } else {
            cfg.learners_.insert(id);
        }

        // Initializing the Progress with the last index means that the follower
        // can be probed (with the last index).
        //
        // TODO(tbg): seems awfully optimistic. Using the first index would be
        // better. The general expectation here is that the follower has no log
        // at all (and will thus likely need a snapshot), though the app may
        // have applied a snapshot out of band before adding the replica (thus
        // making the first index the better choice).

        // When a node is first added, we should mark it as recently active.
        // Otherwise, CheckQuorum may cause us to step down if it is invoked
        // before the added node has had a chance to communicate with us.
        p = tracker_.create(lastIndex_, true);
        return p;
    }

    // makeLearner makes the given ID a learner or stages it to be a learner once
    // an active joint configuration is exited.
    //
    // The former happens when the peer is not a part of the outgoing config, in
    // which case we either add a new learner or demote a voter in the incoming
    // config.
    //
    // The latter case occurs when the configuration is joint and the peer is a
    // voter in the outgoing config. In that case, we do not want to add the peer
    // as a learner because then we'd have to track a peer as a voter and learner
    // simultaneously. Instead, we add the learner to LearnersNext, so that it will
    // be added to Learners the moment the outgoing config is removed by
    // LeaveJoint().
    void makeLearner(tracker::Config& cfg, tracker::ProgressMap& trk, NodeId id)
    {

        auto p = initProgress(cfg, trk, id, true);

        if (cfg.learners_.contains(id)) {
            return;
        }

        // Remove any existing voter in the incoming config...
        cfg.voters_.incoming().remove(id);
        cfg.learners_.erase(id);
        cfg.learnersNext_.erase(id);

        // Use LearnersNext if we can't add the learner to Learners directly, i.e.
        // if the peer is still tracked as a voter in the outgoing config. It will
        // be turned into a learner in LeaveJoint().
        //
        // Otherwise, add a regular learner right away.
        if (cfg.voters_.outgoing().contains(id)) {
            cfg.learnersNext_.insert(id);
        } else {
            cfg.learners_.insert(id);
        }
    }

    // remove this peer as a voter or learner from the incoming config.
    void remove(tracker::Config& cfg, tracker::ProgressMap& trk, NodeId id)
    {
        auto iter = trk.find(id);
        if (iter == trk.end()) {
            return;
        }
        cfg.voters_.incoming().remove(id);
        cfg.learners_.erase(id);
        cfg.learnersNext_.erase(id);

        // If the peer is still a voter in the outgoing config, keep the Progress.
        if (!cfg.voters_.outgoing().contains(id)) {
            trk.erase(id);
        }
    }

    // Describe prints the type and NodeID of the configuration changes as a
    // space-delimited string.
    // std::string Describe(std::vector<ConfChangeSingle>& css)
    //{
    // var buf strings.Builder
    // for _, cc := range ccs {
    //	if( buf.Len() > 0 ){
    //		buf.WriteByte(' ')
    //	}
    //	fmt.Fprintf(&buf, "%s(%d)", cc.Type, cc.NodeID)
    // }
    // return buf.String()
    //}
};

// toConfChangeSingle translates a conf state into 1) a slice of operations creating
// first the config that will become the outgoing one, and then the incoming one, and
// b) another slice that, when applied to the config resulted from 1), represents the
// ConfState.
void toConfChangeSingle(const ConfState& cs, std::vector<ConfChangeItem>& incoming,
                        std::vector<ConfChangeItem>& outgoing)
{
    // Example to follow along this code:
    // voters=(1 2 3) learners=(5) outgoing=(1 2 4 6) learners_next=(4)
    //
    // This means that before entering the joint config, the configuration
    // had voters (1 2 4 6) and perhaps some learners that are already gone.
    // The new set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6)
    // are no longer voters; however 4 is poised to become a learner upon leaving
    // the joint state.
    // We can't tell whether 5 was a learner before entering the joint config,
    // but it doesn't matter (we'll pretend that it wasn't).
    //
    // The code below will construct
    // outgoing = add 1; add 2; add 4; add 6
    // incoming = remove 1; remove 2; remove 4; remove 6
    //            add 1;    add 2;    add 3;
    //            add-learner 5;
    //            add-learner 4;
    //
    // So, when starting with an empty config, after applying 'outgoing' we have
    //
    //   quorum=(1 2 4 6)
    //
    // From which we enter a joint state via 'incoming'
    //
    //   quorum=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)
    //
    // as desired.

    auto add = [](std::vector<ConfChangeItem>& cc, auto& ids, ConfChangeType type) {
        for (auto id : ids) {
            ConfChangeItem ccs;
            ccs.type = type;
            ccs.nodeId = id;
            cc.push_back(ccs);
        }
    };

    // If there are outgoing voters, first add them one by one so that the
    // (non-joint) config has them all.
    add(outgoing, cs.votersOutgoing, AddNode);

    // We're done constructing the outgoing slice, now on to the incoming one
    // (which will apply on top of the config created by the outgoing slice).

    // First, we'll remove all of the outgoing voters.
    // If there are outgoing voters, first add them one by one so that the
    // (non-joint) config has them all.
    add(incoming, cs.votersOutgoing, RemoveNode);

    // Then we'll add the incoming voters and learners.
    // If there are outgoing voters, first add them one by one so that the
    // (non-joint) config has them all.
    add(incoming, cs.voters, AddNode);

    add(incoming, cs.learners, AddLearnerNode);

    // Same for LearnersNext; these are nodes we want to be learners but which
    // are currently voters in the outgoing config.
    // If there are outgoing voters, first add them one by one so that the
    // (non-joint) config has them all.
    add(incoming, cs.learnersNext, AddLearnerNode);
}

// Restore takes a Changer (which must represent an empty configuration), and
// runs a sequence of changes enacting the configuration described in the
// ConfState.
//
// TODO(tbg) it's silly that this takes a Changer. Unravel this by making sure
// the Changer only needs a ProgressMap (not a whole Tracker) at which point
// this can just take LastIndex and MaxInflight directly instead and cook up
// the results from that alone.
Result<ChangerResult, Error> restore(const ConfState& cs, tracker::ProgressTracker& tracker, Index lastIndex)
{
    std::vector<ConfChangeItem> incoming, outgoing;
    toConfChangeSingle(cs, incoming, outgoing);

    Changer chg{ tracker, lastIndex };

    if (outgoing.empty()) {
        // No outgoing config, so just apply the incoming changes one by one.
        for (auto& cs : incoming) {
            std::span<ConfChangeItem> s{ &cs, 1 };
            auto res = chg.simple(s);
            if (res.has_error()) {
                return res.error();
            }
            chg.config_ = std::move(res->config_);
            chg.prs_ = std::move(res->progress_);
        }
    } else {
        // The ConfState describes a joint configuration.
        //
        // First, apply all of the changes of the outgoing config one by one, so
        // that it temporarily becomes the incoming active config. For example,
        // if the config is (1 2 3)&(2 3 4), this will establish (2 3 4)&().
        for (auto& cs : outgoing) {
            std::span<ConfChangeItem> s{ &cs, 1 };
            auto res = chg.simple(s);
            if (res.has_error()) {
                return res.error();
            }
            chg.config_ = std::move(res->config_);
            chg.prs_ = std::move(res->progress_);
        }

        // Now enter the joint state, which rotates the above additions into the
        // outgoing config, and adds the incoming config in. Continuing the
        // example above, we'd get (1 2 3)&(2 3 4), i.e. the incoming operations
        // would be removing 2,3,4 and then adding in 1,2,3 while transitioning
        // into a joint state.
        auto res = chg.enterJoint(cs.autoLeave, incoming);
        if (res.has_error()) {
            return res.error();
        }
        chg.config_ = std::move(res->config_);
        chg.prs_ = std::move(res->progress_);
    }
    return ChangerResult{ std::move(chg.config_), std::move(chg.prs_) };
}

} // namespace confchange
} // namespace raft