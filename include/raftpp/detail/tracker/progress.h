#pragma once

#include <map>
#include <memory>
#include <utility>

#include <raftpp/detail/message.h>
#include <raftpp/detail/tracker/inflights.h>

namespace raft {
namespace tracker {

enum StateType : uint32_t
{
    // StateProbe indicates a follower whose last index isn't known. Such a
    // follower is "probed" (i.e. an append sent periodically) to narrow down
    // its last index. In the ideal (and common) case, only one round of probing
    // is necessary as the follower will react with a hint. Followers that are
    // probed over extended periods of time are often offline.
    StateProbe = 0,
    // StateReplicate is the state steady in which a follower eagerly receives
    // log entries to append to its log.
    StateReplicate,
    // StateSnapshot indicates a follower that needs log entries not available
    // from the leader's Raft log. Such a follower needs a full snapshot to
    // return to StateReplicate.
    StateSnapshot,
};

// Progress represents a follower’s progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tbg): Progress is basically a state machine whose transitions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal.
class Progress
{
public:
    Progress(Index lastIndex, size_t maxInflight, size_t maxInflightBytes, bool recentActive)
      : match_(0)
      , next_(lastIndex)
      , state_(StateProbe)
      , pendingSnapshot_(0)
      , recentActive_(recentActive)
      , paused_(false)
      , inflights_(maxInflight, maxInflightBytes)
    {
    }

    // resetState moves the Progress into the specified State, resetting MsgAppFlowPaused,
    // PendingSnapshot, and Inflights.
    void resetState(StateType state)
    {
        paused_ = false;
        pendingSnapshot_ = 0;
        state_ = state;
        inflights_.reset();
    }

    // BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
    // optionally and if larger, the index of the pending snapshot.
    void becomeProbe()
    {
        // If the original state is StateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if (state_ == StateSnapshot) {
            auto pendingSnapshot = pendingSnapshot_;
            resetState(StateProbe);
            next_ = std::max(match_ + 1, pendingSnapshot + 1);
        } else {
            resetState(StateProbe);
            next_ = match_ + 1;
        }
    }

    // BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
    void becomeReplicate()
    {
        resetState(StateReplicate);
        next_ = match_ + 1;
    }

    // BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
    // snapshot index.
    void becomeSnapshot(Index snapshotIndex)
    {
        resetState(StateSnapshot);
        pendingSnapshot_ = snapshotIndex;
    }

    // UpdateOnEntriesSend updates the progress on the given number of consecutive
    // entries being sent in a MsgAppend, with the given total bytes size, appended at
    // and after the given log index.
    void sentEntries(size_t entries, size_t bytes, Index nextIndex)
    {
        switch (state_) {
        case StateReplicate:
            if (entries > 0) {
                auto last = nextIndex + entries - 1;
                // appends all the way up to and including index last are in-flight.
                next_ = last + 1;
                inflights_.add(last, bytes);
            }
            // If this message overflows the in-flights tracker, or it was already full,
            // consider this message being a probe, so that the flow is paused.
            paused_ = inflights_.full();
            break;
        case StateProbe:
            // TODO(pavelkalinnikov): this condition captures the previous behaviour,
            // but we should set MsgAppFlowPaused unconditionally for simplicity, because any
            // MsgAppend in StateProbe is a probe, not only non-empty ones.
            if (entries > 0) {
                paused_ = true;
            }
            break;
        default:
            panic("sending append in unhandled state");
        }
    }

    // update is called when an MsgAppendResponse arrives from the follower, with the
    // index acked by it. The method returns false if the given n index comes from
    // an outdated message. Otherwise it updates the progress and returns true.
    bool update(Index idx)
    {
        next_ = std::max(next_, idx + 1);
        if (match_ < idx) {
            match_ = idx;
            paused_ = false;
            return true;
        }
        return false;
    }

    // MaybeDecrTo adjusts the Progress to the receipt of a MsgAppend rejection. The
    // arguments are the index of the append message rejected by the follower, and
    // the hint that we want to decrease to.
    //
    // Rejections can happen spuriously as messages are sent out of order or
    // duplicated. In such cases, the rejection pertains to an index that the
    // Progress already knows were previously acknowledged, and false is returned
    // without changing the Progress.
    //
    // If the rejection is genuine, Next is lowered sensibly, and the Progress is
    // cleared for sending log entries.
    bool maybeDecrTo(Index rejected, Index matchHint)
    {
        if (state_ == StateReplicate) {
            // The rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if (rejected <= match_) {
                return false;
            }
            next_ = match_ + 1;
            return true;
        }

        // The rejection must be stale if "rejected" does not match next - 1. This
        // is because non-replicating followers are probed one entry at a time.
        if (next_ - 1 != rejected) {
            return false;
        }

        next_ = std::max(std::min(rejected, matchHint + 1), Index{ 1 });
        paused_ = false;
        return true;
    }

    // IsPaused returns whether sending log entries to this node has been throttled.
    // This is done when a node has rejected recent MsgApps, is currently waiting
    // for a snapshot, or has reached the MaxInflightMsgs limit. In normal
    // operation, this is false. A throttled node will be contacted less frequently
    // until it has reached a state in which it's able to accept a steady stream of
    // log entries again.
    bool isPaused()
    {
        switch (state_) {
        case StateProbe:
        case StateReplicate:
            return paused_;
        case StateSnapshot:
            return true;
        default:
            panic("bad progress state");
        }
    }

    inline Index match() const { return match_; }

    inline Index next() const { return next_; }

    inline StateType state() const { return state_; }

    inline bool recentActive() const { return recentActive_; }

    inline Inflights& inflights() { return inflights_; }

    inline void pause() { paused_ = true; }

    inline void resume() { paused_ = false; }

    inline void setRecentActive(bool b) { recentActive_ = b; }

    void reset(Index match, Index next)
    {
        match_ = match;
        next_ = next;

        state_ = StateProbe;
        pendingSnapshot_ = 0;
        recentActive_ = false;
        paused_ = false;
        inflights_.reset();
    }

private:
    Index match_;

    Index next_;
    // State defines how the leader should interact with the follower.
    //
    // When in StateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in StateReplicate, leader optimistically increases next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in StateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    StateType state_;

    // PendingSnapshot is used in StateSnapshot and tracks the last index of the
    // leader at the time at which it realized a snapshot was necessary. This
    // matches the index in the MsgSnapshot message emitted from raft.
    //
    // While there is a pending snapshot, replication to the follower is paused.
    // The follower will transition back to StateReplicate if the leader
    // receives an MsgAppendResponse from it that reconnects the follower to the
    // leader's log (such an MsgAppendResponse is emitted when the follower applies a
    // snapshot). It may be surprising that PendingSnapshot is not taken into
    // account here, but consider that complex systems may delegate the sending
    // of snapshots to alternative datasources (i.e. not the leader). In such
    // setups, it is difficult to manufacture a snapshot at a particular index
    // requested by raft and the actual index may be ahead or behind. This
    // should be okay, as long as the snapshot allows replication to resume.
    //
    // The follower will transition to StateProbe if ReportSnapshot is called on
    // the leader; if SnapshotFinish is passed then PendingSnapshot becomes the
    // basis for the next attempt to append. In practice, the first mechanism is
    // the one that is relevant in most cases. However, if this MsgAppendResponse is
    // lost (fallible network) then the second mechanism ensures that in this
    // case the follower does not erroneously remain in StateSnapshot.
    Index pendingSnapshot_;

    // RecentActive is true if the progress is recently active. Receiving any messages
    // from the corresponding follower indicates the progress is active.
    // RecentActive can be reset to false after an election timeout.
    // This is always true on the leader.
    bool recentActive_;

    // paused_ is used when the MsgAppend flow to a node is throttled. This
    // happens in StateProbe, or StateReplicate with saturated Inflights. In both
    // cases, we need to continue sending MsgAppend once in a while to guarantee
    // progress, but we only do so when MsgAppFlowPaused is false (it is reset on
    // receiving a heartbeat response), to not overflow the receiver. See
    // IsPaused().
    bool paused_;

    // Inflights is a sliding window for the inflight messages.
    // Each inflight message contains one or more log entries.
    // The max number of entries per message is defined in raft config as MaxSizePerMsg.
    // Thus inflight effectively limits both the number of inflight messages
    // and the bandwidth each Progress can use.
    // When inflights is Full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.FreeLE with the index of the last
    // received entry.
    Inflights inflights_;
};

using ProgressPtr = std::shared_ptr<Progress>;

using ProgressMap = std::map<NodeId, ProgressPtr>;

} // namespace tracker
} // namespace raft

template <>
struct std::formatter<raft::tracker::StateType> : std::formatter<std::string_view>
{
    inline auto format(raft::tracker::StateType st, format_context& ctx) const
    {
        const static std::array<std::string, 3> names = {
            "StateProbe",
            "StateReplicate",
            "StateSnapshot",
        };
        return std::format_to(ctx.out(), "{}", names[st]);
    }
};