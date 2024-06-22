#pragma once

#include <algorithm>
#include <array>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <raftpp/detail/confchange.h>
#include <raftpp/detail/log.h>
#include <raftpp/detail/message.h>
#include <raftpp/detail/quorum.h>
#include <raftpp/detail/readonly.h>
#include <raftpp/detail/result.h>
#include <raftpp/detail/storage.h>
#include <raftpp/detail/tracker/tracker.h>
#include <raftpp/detail/utils.h>
#include <spdlog/spdlog.h>

namespace raft {

// Possible values for StateType.
enum StateType : uint32_t
{
    Follower = 0,
    Candidate,
    Leader,
    PreCandidate,
    NumStates,
};

constexpr size_t noLimit = std::numeric_limits<size_t>::max();

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
struct SoftState
{
    NodeId lead_;
    StateType state_;

    auto operator<=>(const SoftState&) const = default;
};

using Logger = ::spdlog::logger;
// Config contains the parameters to start a raft.
struct Config
{
    Config(Logger& logger)
      : logger_(logger)
      , id_(0)
      , electionTick_(50)
      , heartbeatTick_(5)
      , applied_(0)
      , maxSizePerMsg_(10000000)
      , maxCommittedSizePerReady_(0)
      , maxUncommittedEntriesSize_(0)
      , maxInflightMsgs_(10000)
      , maxInflightBytes_(10000000)
      , checkQuorum_(false)
      , preVote_(true)
      , readOnlyOption_(ReadIndexSafe)
      , disableProposalForwarding_(false)
      , disableConfChangeValidation_(false)
      , stepDownOnRemoval_(false)
    {
    }

    // Logger is the logger used for raft log. For multinode which can host
    // multiple raft group, each raft group can have its own logger
    Logger& logger_;

    // ID is the identity of the local raft. ID cannot be 0.
    NodeId id_;

    // ElectionTick is the number of Node.Tick invocations that must pass between
    // elections. That is, if a follower does not receive any message from the
    // leader of current term before ElectionTick has elapsed, it will become
    // candidate and start an election. ElectionTick must be greater than
    // HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
    // unnecessary leader switching.
    size_t electionTick_;
    // HeartbeatTick is the number of Node.Tick invocations that must pass between
    // heartbeats. That is, a leader sends heartbeat messages to maintain its
    // leadership every HeartbeatTick ticks.
    size_t heartbeatTick_;

    // Storage is the storage for raft. raft generates entries and states to be
    // stored in storage. raft reads the persisted entries and states out of
    // Storage when it needs. raft reads out the previous state and configuration
    // out of storage when restarting.

    // Applied is the last applied index. It should only be set when restarting
    // raft. raft will not return entries to the application smaller or equal to
    // Applied. If Applied is unset when restarting, raft might return previous
    // applied entries. This is a very application dependent configuration.
    Index applied_;

    // AsyncStorageWrites configures the raft node to write to its local storage
    // (raft log and state machine) using a request/response message passing
    // interface instead of the default Ready/Advance function call interface.
    // Local storage messages can be pipelined and processed asynchronously
    // (with respect to Ready iteration), facilitating reduced interference
    // between Raft proposals and increased batching of log appends and state
    // machine application. As a result, use of asynchronous storage writes can
    // reduce end-to-end commit latency and increase maximum throughput.
    //
    // When true, the Ready.Message slice will include MsgStorageAppend and
    // MsgStorageApply messages. The messages will target a LocalAppendThread
    // and a LocalApplyThread, respectively. Messages to the same target must be
    // reliably processed in order. In other words, they can't be dropped (like
    // messages over the network) and those targeted at the same thread can't be
    // reordered. Messages to different targets can be processed in any order.
    //
    // MsgStorageAppend carries Raft log entries to append, election votes /
    // term changes / updated commit indexes to persist, and snapshots to apply.
    // All writes performed in service of a MsgStorageAppend must be durable
    // before response messages are delivered. However, if the MsgStorageAppend
    // carries no response messages, durability is not required. The message
    // assumes the role of the Entries, HardState, and Snapshot fields in Ready.
    //
    // MsgStorageApply carries committed entries to apply. Writes performed in
    // service of a MsgStorageApply need not be durable before response messages
    // are delivered. The message assumes the role of the CommittedEntries field
    // in Ready.
    //
    // Local messages each carry one or more response messages which should be
    // delivered after the corresponding storage write has been completed. These
    // responses may target the same node or may target other nodes. The storage
    // threads are not responsible for understanding the response messages, only
    // for delivering them to the correct target after performing the storage
    // write.
    // bool asyncStorageWrites_;

    // MaxSizePerMsg limits the max byte size of each append message. Smaller
    // value lowers the raft recovery cost(initial probing and message lost
    // during normal operation). On the other side, it might affect the
    // throughput during normal replication. Note: math.Maxuint64_t for unlimited,
    // 0 for at most one entry per message.
    size_t maxSizePerMsg_;
    // MaxCommittedSizePerReady limits the size of the committed entries which
    // can be applying at the same time.
    //
    // Despite its name (preserved for compatibility), this quota applies across
    // Ready structs to encompass all outstanding entries in unacknowledged
    // MsgStorageApply messages when AsyncStorageWrites is enabled.
    size_t maxCommittedSizePerReady_;
    // MaxUncommittedEntriesSize limits the aggregate byte size of the
    // uncommitted entries that may be appended to a leader's log. Once this
    // limit is exceeded, proposals will begin to return ErrProposalDropped
    // errors. Note: 0 for no limit.
    size_t maxUncommittedEntriesSize_;
    // MaxInflightMsgs limits the max number of in-flight append messages during
    // optimistic replication phase. The application transportation layer usually
    // has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
    // overflowing that sending buffer. TODO: feedback to application to
    // limit the proposal rate?
    size_t maxInflightMsgs_;
    // MaxInflightBytes limits the number of in-flight bytes in append messages.
    // Complements MaxInflightMsgs. Ignored if zero.
    //
    // This effectively bounds the bandwidth-delay product. Note that especially
    // in high-latency deployments setting this too low can lead to a dramatic
    // reduction in throughput. For example, with a peer that has a round-trip
    // latency of 100ms to the leader and this setting is set to 1 MB, there is a
    // throughput limit of 10 MB/s for this group. With RTT of 400ms, this drops
    // to 2.5 MB/s. See Little's law to understand the maths behind.
    size_t maxInflightBytes_;

    // CheckQuorum specifies if the leader should check quorum activity. Leader
    // steps down when quorum is not active for an electionTimeout.
    bool checkQuorum_;

    // PreVote enables the Pre-Vote algorithm described in raft thesis section
    // 9.6. This prevents disruption when a node that has been partitioned away
    // rejoins the cluster.
    bool preVote_;

    // ReadOnlyOption specifies how the read only request is processed.
    //
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    //
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    // CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
    ReadOnlyOption readOnlyOption_;

    // DisableProposalForwarding set to true means that followers will drop
    // proposals, rather than forwarding them to the leader. One use case for
    // this feature would be in a situation where the Raft leader is used to
    // compute the data of a proposal, for example, adding a timestamp from a
    // hybrid logical clock to data in a monotonically increasing way. Forwarding
    // should be disabled to prevent a follower with an inaccurate hybrid
    // logical clock from assigning the timestamp and then forwarding the data
    // to the leader.
    bool disableProposalForwarding_;

    // DisableConfChangeValidation turns off propose-time verification of
    // configuration changes against the currently active configuration of the
    // raft instance. These checks are generally sensible (cannot leave a joint
    // config unless in a joint config, et cetera) but they have false positives
    // because the active configuration may not be the most recent
    // configuration. This is because configurations are activated during log
    // application, and even the leader can trail log application by an
    // unbounded number of entries.
    // Symmetrically, the mechanism has false negatives - because the check may
    // not run against the "actual" config that will be the predecessor of the
    // newly proposed one, the check may pass but the new config may be invalid
    // when it is being applied. In other words, the checks are best-effort.
    //
    // Users should *not* use this option unless they have a reliable mechanism
    // (above raft) that serializes and verifies configuration changes. If an
    // invalid configuration change enters the log and gets applied, a panic
    // will result.
    //
    // This option may be removed once false positives are no longer possible.
    // See: https://github.com/etcd-io/raft/issues/80
    bool disableConfChangeValidation_;

    // StepDownOnRemoval makes the leader step down when it is removed from the
    // group or demoted to a learner.
    //
    // This behavior will become unconditional in the future. See:
    // https://github.com/etcd-io/raft/issues/83
    bool stepDownOnRemoval_;

    Result<void, Error> validate()
    {
        if (id_ == InvalidId) {
            return Error::fmt("cannot use none as id");
        }

        if (heartbeatTick_ == 0) {
            return Error::fmt("heartbeat tick must be greater than 0");
        }

        if (electionTick_ <= heartbeatTick_) {
            return Error::fmt("election tick must be greater than heartbeat tick");
        }

        if (maxUncommittedEntriesSize_ == 0) {
            maxUncommittedEntriesSize_ = noLimit;
        }

        // default MaxCommittedSizePerReady to MaxSizePerMsg because they were
        // previously the same parameter.
        if (maxCommittedSizePerReady_ == 0) {
            maxCommittedSizePerReady_ = maxSizePerMsg_;
        }

        if (maxInflightMsgs_ == 0) {
            return Error::fmt("max inflight messages must be greater than 0");
        }
        if (maxInflightBytes_ == 0) {
            maxInflightBytes_ = noLimit;
        } else if (maxInflightBytes_ < maxSizePerMsg_) {
            return Error::fmt("max inflight bytes must be >= max message size");
        }

        if (readOnlyOption_ == ReadIndexLeaseBased && !checkQuorum_) {
            return Error::fmt("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased");
        }

        return {};
    }
};

template <Storage T>
class Node;

template <Storage T>
class Raft
{
    const struct ConfigValidator
    {
        ConfigValidator(Config& c) { c.validate().unwrap(); }
    } validator_;

public:
    Raft(Config& c, T& storage)
      : validator_(c)
      , logger_(c.logger_)
      , log_(storage, c.maxCommittedSizePerReady_)
      , readOnly_(c.readOnlyOption_)
      , tracker_(c.maxInflightMsgs_, c.maxInflightBytes_)
      , id_(c.id_)
      , isLearner_(false)
      , maxMsgSize_(c.maxSizePerMsg_)
      , maxUncommittedSize_(c.maxUncommittedEntriesSize_)
      , electionTimeout_(c.electionTick_)
      , heartbeatTimeout_(c.heartbeatTick_)
      , checkQuorum_(c.checkQuorum_)
      , preVote_(c.preVote_)
      , disableProposalForwarding_(c.disableProposalForwarding_)
      , disableConfChangeValidation_(c.disableConfChangeValidation_)
      , stepDownOnRemoval_(c.stepDownOnRemoval_)
      , electionElapsed_(0)
      , heartbeatElapsed_(0)
      , rng_(c.electionTick_, c.electionTick_ * 2)
    {
        auto state = storage.initialState().unwrap();
        auto changeResult = confchange::restore(state.conf, tracker_, log_.lastIndex()).unwrap();

        if (state.conf != switchToConfig(changeResult)) {
            panic("mismatching confstate after restore");
        }

        if (!state.hard.empty()) {
            loadState(state.hard);
        }

        if (c.applied_ > 0) {
            log_.appliedTo(c.applied_, 0);
        }

        becomeFollower(term_, InvalidId);
    }

    NodeId id() const { return id_; }

    StateType state() const { return state_; }

    bool hasLeader() const { return lead_ != InvalidId; }

    SoftState softState() const { return SoftState{ lead_, state_ }; }

    HardState hardState() const
    {
        return {
            .term = term_,
            .commit = log_.committed_,
            .vote = vote_,
        };
    }

    ConfState applyConfChange(const ConfChange& cc)
    {
        Result<confchange::ChangerResult, Error> result;

        auto changer = confchange::Changer{
            tracker_,
            log_.lastIndex(),
        };

        if (confchange::leaveJoint(cc)) {
            result = changer.leaveJoint();
        } else if (auto [autoLeave, ok] = confchange::enterJoint(cc); ok) {
            result = changer.enterJoint(autoLeave, cc.changes);
        } else {
            result = changer.simple(cc.changes);
        }

        return switchToConfig(result.unwrap());
    }

    Result<> propose(std::string data)
    {
        return step(ProposalRequst{
          .from = id_,
          .entries = { Entry{
            .type = EntryNormal,
            .data = std::move(data),
          } },
        });
    }

    Result<> proposeConfChange(const ConfChange& cc)
    {
        return step(ProposalRequst{
          .from = id_,
          .entries = { Entry{
            .type = EntryConfChange,
            .data = cc.serialize(),
          } },
        });
    }

    template <Message Msg>
    Result<> step(Msg&& m)
    {
        // skip message dosen't have term
        if constexpr (!one_of<Msg, ProposalRequst, ReadIndexRequest>) {
            if (m.term < term_) {
                if constexpr (one_of<Msg, AppendEntriesRequest, HeartbeatRequest>) {
                    if (checkQuorum_ || preVote_) {
                        // We have received messages from a leader at a lower term. It is possible
                        // that these messages were simply delayed in the network, but this could
                        // also mean that this node has advanced its term number during a network
                        // partition, and it is now unable to either win an election or to rejoin
                        // the majority on the old term. If checkQuorum is false, this will be
                        // handled by incrementing term numbers in response to MsgRequestVote with a
                        // higher term, but if checkQuorum is true we may not advance the term on
                        // MsgRequestVote and must generate other messages to advance the term. The net
                        // result of these two features is to minimize the disruption caused by
                        // nodes that have been removed from the cluster's configuration: a
                        // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                        // but it will not receive MsgAppend or MsgHeartbeat, so it will not create
                        // disruptive term increases, by notifying leader of this node's activeness.
                        // The above comments also true for Pre-Vote
                        //
                        // When follower gets isolated, it soon starts an election ending
                        // up with a higher term than leader, although it won't receive enough
                        // votes to win the election. When it regains connectivity, this response
                        // with "MsgAppendResponse" of higher term would force leader to step down.
                        // However, this disruption is inevitable to free this stuck node with
                        // fresh election. This can be prevented with Pre-Vote phase.
                        send(AppendEntriesResponse{
                          .to = m.from,
                          .reject = true,
                        });
                    }
                } else if constexpr (one_of<Msg, VoteRequest>) {
                    // Before Pre-Vote enable, there may have candidate with higher term,
                    // but less log. After update to Pre-Vote, the cluster may deadlock if
                    // we drop messages with a lower term.
                    if (m.pre) {
                        send(VoteResponse{
                          .to = m.from,
                          .pre = m.pre,
                          .term = term_,
                          .reject = true,
                        });
                    }
                }
                // ignore other cases
                return {};
            }

            // now message's term >= node's term_
            if (m.term > term_) {
                if constexpr (one_of<Msg, VoteRequest>) {
                    bool force = (m.type == CampaignTransfer);
                    bool inLease = checkQuorum_ && lead_ != InvalidId && electionElapsed_ < electionTimeout_;
                    if (!force && inLease) {
                        // If a server receives a RequestVote request within the minimum election timeout
                        // of hearing from a current leader, it does not update its term or grant its vote
                        return {};
                    }

                    // Never change our term in response to a PreVote
                    if (!m.pre) {
                        becomeFollower(m.term, InvalidId);
                    }
                } else if constexpr (one_of<Msg, HeartbeatRequest, AppendEntriesRequest, InstallSnapshotRequest>) {
                    becomeFollower(m.term, m.from);
                } else if constexpr (one_of<Msg, VoteResponse>) {
                    // We send pre-vote requests with a term in our future. If the
                    // pre-vote is granted, we will increment our term when we get a
                    // quorum. If it is not, the term comes from the node that
                    // rejected our vote so we should become a follower at the new
                    // term.
                    if (m.reject) {
                        becomeFollower(m.term, InvalidId);
                    }
                } else {
                    becomeFollower(m.term, InvalidId);
                }
            }
        }

        if constexpr (one_of<Msg, VoteRequest>) {

            // We can vote if this is a repeat of a vote we've already cast...
            bool granted = (vote_ == m.from ||
                            // ...we haven't voted and we don't think there's a leader yet in this term...
                            (vote_ == InvalidId && lead_ == InvalidId) ||
                            // ...or this is a PreVote for a future term...
                            (m.pre && m.term > term_)) &&
              // up to date
              log_.isUpToDate(m.lastLog.index, m.lastLog.term);

            // Note: it turns out that that learners must be allowed to cast votes.
            // This seems counter- intuitive but is necessary in the situation in which
            // a learner has been promoted (i.e. is now a voter) but has not learned
            // about this yet.
            // For example, consider a group in which id=1 is a learner and id=2 and
            // id=3 are voters. A configuration change promoting 1 can be committed on
            // the quorum `{2,3}` without the config change being appended to the
            // learner's log. If the leader (say 2) fails, there are de facto two
            // voters remaining. Only 3 can win an election (due to its log containing
            // all committed entries), but to do so it will need 1 to vote. But 1
            // considers itself a learner and will continue to do so until 3 has
            // stepped up as leader, replicates the conf change to 1, and 1 applies it.
            // Ultimately, by receiving a request to vote, the learner realizes that
            // the candidate believes it to be a voter, and that it should act
            // accordingly. The candidate's config may be stale, too; but in that case
            // it won't win the election, at least in the absence of the bug discussed
            // in:
            // https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.

            // When responding to Msg{Pre,}Vote messages we include the term
            // from the message, not the local term. To see why, consider the
            // case where a single node was previously partitioned away and
            // it's local term is now out of date. If we include the local term
            // (recall that for pre-votes we don't update the local term), the
            // (pre-)campaigning node on the other end will proceed to ignore
            // the message (it ignores all out of date messages).
            // The term in the original message and current local term are the
            // same in the case of regular votes, but different for pre-votes.
            send(VoteResponse{
              .to = m.from,
              .pre = m.pre,
              .term = m.term,
              .reject = !granted,
            });

            if (!m.pre && granted) {
                // Only record real votes.
                electionElapsed_ = 0;
                vote_ = m.from;
            }
            return {};
        }

        switch (state_) {
        case Leader:
            return stepLeader(m);
        case Candidate:
        case PreCandidate:
            return stepCandidate(m);
        case Follower:
            return stepFollower(m);
        default:
            panic("invalid state: ", int(state_));
        }
    }

private:
    // send schedules persisting state to a stable storage and AFTER that
    // sending the message (as part of next Ready message processing).
    template <Message Msg>
    void send(Msg&& m)
    {
        m.from = id_;
        if constexpr (one_of<Msg, VoteRequest, VoteResponse>) {
            if (m.term == 0) {
                panic("term should be set when sending {}, term: {}", typeid(m).name(), term_);
            }
        } else if constexpr (!one_of<Msg, ProposalRequst, ReadIndexRequest>) {
            if (m.term != 0) {
                panic("term should not be set when sending {} (was {})", typeid(m).name(), m.term);
            }
            m.term = term_;
        }

        msgs_.push_back(std::forward<Msg>(m));
    }

    // switchToConfig reconfigures this node to use the provided configuration. It
    // updates the in-memory state and, when necessary, carries out additional
    // actions such as reacting to the removal of nodes or changed quorum
    // requirements.
    //
    // The inputs usually result from restoring a ConfState or applying a ConfChange.
    ConfState switchToConfig(confchange::ChangerResult& res)
    {
        tracker_.reset(std::move(res.config_), std::move(res.progress_));
        tracker_.committedIndex();
        // logger_.info("{} switched to configuration {}", id_, tracker_.Config)
        auto cs = tracker_.confState();
        auto progress = tracker_.getProgress(id_);

        // Update whether the node itself is a learner, resetting to false when the
        // node is removed.
        isLearner_ = (progress != nullptr && tracker_.config().learners_.contains(id_));

        if ((progress == nullptr || isLearner_) && state_ == Leader) {
            // This node is leader and was removed or demoted, step down if requested.
            //
            // We prevent demotions at the time writing but hypothetically we handle
            // them the same way as removing the leader.
            //
            // TODO(tbg): ask follower with largest Match to TimeoutNow (to avoid
            // interruption). This might still drop some proposals but it's better than
            // nothing.
            if (stepDownOnRemoval_) {
                becomeFollower(term_, 0);
            }
            return cs;
        }

        // The remaining steps only make sense if this node is the leader and there
        // are other nodes.
        if (state_ != Leader || cs.voters.empty()) {
            return cs;
        }

        if (maybeCommit()) {
            // If the configuration change means that more entries are committed now,
            // broadcast/append to everyone in the updated config.
            bcastAppend();
        } else {
            // Otherwise, still probe the newly added replicas; there's no reason to
            // let them wait out a heartbeat interval (or the next incoming
            // proposal).
            tracker_.visit([&](NodeId id, tracker::Progress&) {
                if (id == id_) {
                    return;
                }
                sendAppend(id, false /* sendIfEmpty */);
            });
        }
        // If the leadTransferee was removed or demoted, abort the leadership transfer.
        if ((leadTransferee_ != InvalidId) && (!tracker_.config().voters_.contains(leadTransferee_))) {
            abortLeaderTransfer();
        }

        return cs;
    }

    // sendAppend sends an append RPC with new entries (if any) and the
    // current commit index to the given peer.
    // sendAppend sends an append RPC with new entries to the given peer,
    // if necessary. Returns true if a message was sent. The sendIfEmpty
    // argument controls whether messages with no entries will be sent
    // ("empty" messages are useful to convey updated Commit indexes, but
    // are undesirable when we're sending multiple messages in a batch).
    bool sendAppend(NodeId to, bool sendIfEmpty = true)
    {
        auto pr = tracker_.getProgress(to);

        if (pr->isPaused()) {
            logger_.debug("paused");
            return false;
        }

        auto lastIndex = pr->next() - 1;
        auto nexIndex = pr->next();

        auto lastTerm = log_.term(lastIndex);

        Result<EntryList> entries;
        // In a throttled StateReplicate only send empty MsgAppend, to ensure progress.
        // Otherwise, if we had a full Inflights and all inflight messages were in
        // fact dropped, replication to that follower would stall. Instead, an empty
        // MsgAppend will eventually reach the follower (heartbeats responses prompt the
        // leader to send an append), allowing it to be acked or rejected, both of
        // which will clear out Inflights.
        if (pr->state() != tracker::StateReplicate || !pr->inflights().full()) {
            entries = log_.entries(nexIndex, maxMsgSize_);
        }

        if ((!entries || entries->empty()) && !sendIfEmpty) {
            return false;
        }

        // send snapshot if we failed to get term or entries
        if (!lastTerm || !entries) {
            if (!pr->recentActive()) {
                return false;
            }

            auto snap = log_.snapshot();
            if (!snap) {
                if (snap.error() == ErrSnapshotTemporarilyUnavailable) {
                    return false;
                }
                snap.unwrap(); // TODO(bdarnell)
            }
            auto snapshot = **snap;

            if (snapshot.empty()) {
                panic("need non-empty snapshot");
            }

            auto sindex = snapshot.meta.index;

            // sindex, sterm = snapshot.meta.Index, snapshot.meta.Term
            pr->becomeSnapshot(sindex);

            send(InstallSnapshotRequest{
              .to = to,
              .snapshot = snapshot,
            });

            return true;
        }

        // Send the actual MsgAppend otherwise, and update the progress accordingly.
        pr->sentEntries(entries->size(), payloadSize(*entries), nexIndex);

        // NB: pr has been updated, but we make sure to only use its old values below.
        send(AppendEntriesRequest{
          .to = to,
          .prevLog = {.index = lastIndex,
          .term = *lastTerm,
	  },
          .entries = std::move(*entries),
          .commit = log_.committed_,
        });

        return true;
    }

    // sendHeartbeat sends a heartbeat RPC to the given peer.
    void sendHeartbeat(NodeId to, const std::string& ctx)
    {
        // Attach the commit as min(to.matched, r.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        auto commit = std::min(tracker_.getProgress(to)->match(), log_.committed_);
        send(HeartbeatRequest{
          .to = to,
          .commit = commit,
          .context = ctx,
        });
    }

    // bcastAppend sends RPC, with entries to all peers that are not up-to-date
    // according to the progress recorded in tracker_.
    void bcastAppend()
    {
        tracker_.visit([&](NodeId id, tracker::Progress&) {
            if (id == id_) {
                return;
            }
            sendAppend(id);
        });
    }

    void bcastHeartbeatWithCtx(const std::string& ctx)
    {
        tracker_.visit([&](NodeId id, tracker::Progress&) {
            if (id == id_) {
                return;
            }
            sendHeartbeat(id, ctx);
        });
    }

    // bcastHeartbeat sends RPC, without entries to all the peers.
    void bcastHeartbeat()
    {
        auto& ctx = readOnly_.lastPendingRequestCtx();
        bcastHeartbeatWithCtx(ctx);
    }

    void appliedTo(Index index, size_t size)
    {
        auto oldApplied = log_.applied_;
        auto newApplied = std::max(index, oldApplied);
        log_.appliedTo(newApplied, size);

        if (tracker_.config().autoLeave_ && newApplied >= pendingConfIndex_ && state_ == Leader) {
            // If the current (and most recent, at least for this leader's term)
            // configuration should be auto-left, initiate that now. We use a
            // nil Data which unmarshals into an empty ConfChangeV2 and has the
            // benefit that appendEntry can never refuse it based on its size
            // (which registers as zero).

            // NB: this proposal can't be dropped due to size, but can be
            // dropped if a leadership transfer is in progress. We'll keep
            // checking this condition on each applied entry, so either the
            // leadership transfer will succeed and the new leader will leave
            // the joint configuration, or the leadership transfer will fail,
            // and we will propose the config change on the next advance.
            proposeConfChange(ConfChange{});

            // if( err = step(m); err != nil ){
            //	logger_.debug("not initiating automatic transition out of joint configuration {}: {}",
            // tracker_.Config, err) } else { 	logger_.info("initiating automatic transition out of joint
            // configuration {}", tracker_.Config)
            // }
        }
    }

    void appliedSnapshot(Index snapshotIndex)
    {
        log_.stableSnapshot(snapshotIndex);
        appliedTo(snapshotIndex, 0);
    }

    // maybeCommit attempts to advance the commit index. Returns true if
    // the commit index changed (in which case the caller should call
    // r.bcastAppend).
    bool maybeCommit()
    {
        auto mci = tracker_.committedIndex();
        return log_.maybeCommit(mci, term_);
    }

    void reset(Term term)
    {
        if (term_ != term) {
            term_ = term;
            vote_ = InvalidId;
        }
        lead_ = InvalidId;

        electionElapsed_ = 0;
        heartbeatElapsed_ = 0;
        randomizedElectionTimeout_ = rng_();

        abortLeaderTransfer();

        tracker_.resetVotes();
        auto lastIndex = log_.lastIndex();
        tracker_.visit([&](NodeId id, tracker::Progress& p) {
            Index match = 0;
            Index next = log_.lastIndex() + 1;
            if (id == id_) {
                match = next - 1;
            }

            p.reset(match, next);
        });

        pendingConfIndex_ = 0;
        uncommittedSize_ = 0;
        readOnly_.reset();
    }

    bool appendEntry(EntrySlice es)
    {
        auto li = log_.lastIndex();
        for (auto i : std::views::iota(0uz, es.size())) {
            es[i].term = term_;
            es[i].index = li + i + 1;
        }
        // Track the size of this uncommitted proposal.
        if (!increaseUncommittedSize(es)) {
            //  Drop the proposal.
            return false;
        }
        // use latest "last" index after truncate/append
        li = log_.append(es);
        // The leader needs to self-ack the entries just appended once they have
        // been durably persisted (since it doesn't send an MsgAppend to itself). This
        // response message will be added to msgsAfterAppend and delivered back to
        // this node after these entries have been written to stable storage. When
        // handled, this is roughly equivalent to:

        send(AppendEntriesResponse{
          .to = id_,
          .index = li,
          .reject = false,
        });

        return true;
    }

    void tick()
    {
        if (state_ != Leader) {
            electionElapsed_++;
            if (promotable() && pastElectionTimeout()) {
                electionElapsed_ = 0;
                hup();
            }
            return;
        }

        heartbeatElapsed_++;
        electionElapsed_++;

        if (electionElapsed_ >= electionTimeout_) {
            electionElapsed_ = 0;
            if (checkQuorum_) {
                checkQuorum();
            }
            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (state_ == Leader && leadTransferee_ != InvalidId) {
                abortLeaderTransfer();
            }
        }

        if (state_ != Leader) {
            return;
        }

        if (heartbeatElapsed_ >= heartbeatTimeout_) {
            heartbeatElapsed_ = 0;
            bcastHeartbeat();
        }
    }

    void becomeCandidate()
    {
        if (state_ == Leader) {
            panic("invalid transition [leader -> candidate]");
        }
        logger_.info("transition [ {} -> {} ], term {}", state_, Candidate, term_);
        reset(term_ + 1);
        vote_ = id_;
        state_ = Candidate;
    }

    void becomePreCandidate()
    {
        if (state_ == Leader) {
            panic("invalid transition [leader -> pre-candidate]");
        }
        logger_.info("transition [ {} -> {} ], term {}", state_, PreCandidate, term_);
        //  Becoming a pre-candidate changes our step functions and state,
        //  but doesn't change anything else. In particular it does not increase
        //  term_ or change vote_.
        tracker_.resetVotes();
        lead_ = InvalidId;
        state_ = PreCandidate;
    }

    void becomeFollower(Term term, NodeId lead)
    {
        logger_.info("transition [ {} -> {} ], term {}", state_, Follower, term_);
        reset(term);
        lead_ = lead;
        state_ = Follower;
    }

    void becomeLeader()
    {
        if (state_ == Follower) {
            panic("invalid transition [follower -> leader]");
        }

        logger_.info("transition [ {} -> {} ], term {}", state_, Leader, term_);
        reset(term_);
        lead_ = id_;
        state_ = Leader;

        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        auto pr = tracker_.getProgress(id_);
        pr->becomeReplicate();
        // The leader always has RecentActive == true; checkQuorum makes sure to
        // preserve this.
        pr->setRecentActive(true);

        // Conservatively set the pendingConfIndex to the last index in the
        // log. There may or may not be a pending config change, but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        pendingConfIndex_ = log_.lastIndex();

        EntryList empty{ Entry{ EntryEmpty } };

        if (!appendEntry(empty)) {
            // This won't happen because we just called reset() above.
            panic("empty entry was dropped");
        }
        // The payloadSize of an empty entry is 0 (see TestPayloadSizeOfEmptyEntry),
        // so the preceding log append does not count against the uncommitted log
        // quota of the new leader. In other words, after the call to appendEntry,
        // uncommittedSize_ is still 0.
    }

    void hup(bool transfer = false)
    {
        CampaignType t = preVote_ ? CampaignPreElection : CampaignPreElection;
        if (transfer) {
            t = CampaignTransfer;
        }

        if (state_ == Leader) {
            return;
        }

        if (!promotable()) {
            return;
        }

        if (hasUnappliedConfChanges()) {
            return;
        }

        campaign(t);
    }

    bool hasUnappliedConfChanges()
    {
        if (log_.applied_ >= log_.committed_) { // in fact applied == committed
            return false;
        }
        // Scan all unapplied committed entries to find a config change. Paginate the
        // scan, to avoid a potentially unlimited memory spike.
        auto lo = log_.applied_ + 1;
        auto hi = log_.committed_ + 1;
        // Reuse the maxApplyingEntsSize limit because it is used for similar purposes
        // (limiting the read of unapplied committed entries) when raft sends entries
        // via the Ready struct for application.
        // TODO(pavelkalinnikov): find a way to budget memory/bandwidth for this scan
        // outside the raft package.
        auto pageSize = log_.maxApplyingEntsSize_;
        auto res = log_.scan(lo, hi, pageSize, [&](EntryList& ents) -> Result<> {
            auto found = (std::find_if(ents.begin(), ents.end(),
                                       [](const Entry& e) { return e.type == EntryConfChange; }) != ents.end());
            if (found) {
                return ErrLogScanBreak;
            }
            return {};
        });

        if (!res && res.error() == ErrLogScanBreak) {
            return true;
        }
        res.unwrap();
        return false;
    }

    // campaign transitions the raft instance to candidate state. This must only be
    // called after verifying that this is a legitimate transition.
    void campaign(CampaignType t)
    {
        // if (!promotable()) {
        //  This path should not be hit (callers are supposed to check), but
        //  better safe than sorry.
        //  r.logger.Warningf("{} is unpromotable; campaign() should have been called", id_)
        //}
        Term term;
        if (t == CampaignPreElection) {
            becomePreCandidate();
            // PreVote RPCs are sent for the next term before we've incremented term_.
            term = term_ + 1;
        } else {
            becomeCandidate();
            term = term_;
        }

        tracker_.visit([&](NodeId id, tracker::Progress& p) {
            if (tracker_.isLearner(id)) {
                return;
            }
            if (id == id_) {
                // The candidate votes for itself and should account for this self
                // vote once the vote has been durably persisted (since it doesn't
                // send a MsgRequestVote to itself). This response message will be added to
                // msgsAfterAppend and delivered back to this node after the vote
                // has been written to stable storage.
                send(VoteResponse{
                  .to = id,
                  .pre = (t == CampaignPreElection),
                  .term = term,
                  .reject = false,
                });
                return;
            }

            send(VoteRequest {
                .to = id,
                .pre = (t == CampaignPreElection),
                .term = term,
                .lastLog = {
                    .index = log_.lastIndex(),
                    .term = log_.lastTerm(),
                },
            });
        });
    }

    template <Message Msg>
    Result<> stepLeader(Msg&& m)
    {
        // These message types do not require any progress for m.from.
        if constexpr (one_of<Msg, ProposalRequst>) {
            if (m.entries.empty()) {
                panic("stepped empty MsgPropose");
            }
            if (!tracker_.contains(id_)) {
                // If we are not currently a member of the range (i.e. this node
                // was removed from the configuration while serving as leader),
                // drop any new proposals.
                return ErrProposalDropped;
            }
            if (leadTransferee_ != InvalidId) {
                return ErrProposalDropped;
            }

            for (auto i : std::views::iota(0uz, m.entries.size())) {
                auto& e = m.entries[i];
                if (e.type != EntryConfChange) {
                    continue;
                }

                ConfChange cc;
                if (!cc.parse(e.data)) {
                    panic("parse confchange failed");
                }

                // if (cc.IsInitialized()) {
                auto alreadyPending = (pendingConfIndex_ > log_.applied_);
                auto alreadyJoint = tracker_.config().voters_.isJoint();
                auto wantsLeaveJoint = cc.changes.empty();

                bool failed = false;
                // var failedCheck string
                if (alreadyPending) {
                    failed = true;
                    logger_.info("possible unapplied conf change at index {} (applied to {})", pendingConfIndex_,
                                 log_.applied_);
                } else if (alreadyJoint && !wantsLeaveJoint) {
                    failed = true;
                    logger_.info("must transition out of joint config first");
                } else if (!alreadyJoint && wantsLeaveJoint) {
                    failed = true;
                    logger_.info("not in joint state; refusing empty conf change");
                }

                if (failed && !disableConfChangeValidation_) {
                    m.entries[i] = { .type = EntryEmpty };
                } else {
                    pendingConfIndex_ = log_.lastIndex() + i + 1;
                }
                //}
            }
            EntrySlice es{ m.entries };

            if (!appendEntry(es)) {
                return ErrProposalDropped;
            }
            bcastAppend();
            return {};
        } else if constexpr (one_of<Msg, ReadIndexRequest>) {
            // only one voting member (the leader) in the cluster
            if (tracker_.isSingleton()) {
                handleReadIndexReady(m, log_.committed_);
                return {};
            }

            // Reject read only request when this leader has not committed any log entry
            // in its term.
            if (!committedEntryInCurrentTerm()) {
                return {};
            }

            handleReadIndex(m);
            return {};
        } else if constexpr (one_of<Msg, AppendEntriesResponse>) {
            auto pr = tracker_.getProgress(m.from);
            if (!pr) {
                return {};
            }
            // NB: this code path is also hit from (*raft).advance, where the leader steps
            // an MsgAppendResponse to acknowledge the appended entries in the last Ready.

            pr->setRecentActive(true);

            if (m.reject) {
                // RejectHint is the suggested next base entry for appending (i.e.
                // we try to append entry RejectHint+1 next), and LogTerm is the
                // term that the follower has at index RejectHint. Older versions
                // of this library did not populate LogTerm for rejections and it
                // is zero for followers with an empty log.
                //
                // Under normal circumstances, the leader's log is longer than the
                // follower's and the follower's log is a prefix of the leader's
                // (i.e. there is no divergent uncommitted suffix of the log on the
                // follower). In that case, the first probe reveals where the
                // follower's log ends (RejectHint=follower's last index) and the
                // subsequent probe succeeds.
                //
                // However, when networks are partitioned or systems overloaded,
                // large divergent log tails can occur. The naive attempt, probing
                // entry by entry in decreasing order, will be the product of the
                // length of the diverging tails and the network round-trip latency,
                // which can easily result in hours of time spent probing and can
                // even cause outright outages. The probes are thus optimized as
                // described below.
                // logger_.debug("{} received MsgAppendResponse(rejected, hint: (index {}, term {})) from {} for index
                // {}",
                //	id_, m.RejectHint, m.logTerm, m.from, m.index)
                auto nextProbeIdx = m.rejectHint.index;
                if (m.rejectHint.term > 0) {
                    // If the follower has an uncommitted log tail, we would end up
                    // probing one by one until we hit the common prefix.
                    //
                    // For example, if the leader has:
                    //
                    //   idx        1 2 3 4 5 6 7 8 9
                    //              -----------------
                    //   term (L)   1 3 3 3 5 5 5 5 5
                    //   term (F)   1 1 1 1 2 2
                    //
                    // Then, after sending an append anchored at (idx=9,term=5) we
                    // would receive a RejectHint of 6 and LogTerm of 2. Without the
                    // code below, we would try an append at index 6, which would
                    // fail again.
                    //
                    // However, looking only at what the leader knows about its own
                    // log and the rejection hint, it is clear that a probe at index
                    // 6, 5, 4, 3, and 2 must fail as well:
                    //
                    // For all of these indexes, the leader's log term is larger than
                    // the rejection's log term. If a probe at one of these indexes
                    // succeeded, its log term at that index would match the leader's,
                    // i.e. 3 or 5 in this example. But the follower already told the
                    // leader that it is still at term 2 at index 6, and since the
                    // log term only ever goes up (within a log), this is a contradiction.
                    //
                    // At index 1, however, the leader can draw no such conclusion,
                    // as its term 1 is not larger than the term 2 from the
                    // follower's rejection. We thus probe at 1, which will succeed
                    // in this example. In general, with this approach we probe at
                    // most once per term found in the leader's log.
                    //
                    // There is a similar mechanism on the follower (implemented in
                    // handleAppendEntries via a call to findConflictByTerm) that is
                    // useful if the follower has a large divergent uncommitted log
                    // tail[1], as in this example:
                    //
                    //   idx        1 2 3 4 5 6 7 8 9
                    //              -----------------
                    //   term (L)   1 3 3 3 3 3 3 3 7
                    //   term (F)   1 3 3 4 4 5 5 5 6
                    //
                    // Naively, the leader would probe at idx=9, receive a rejection
                    // revealing the log term of 6 at the follower. Since the leader's
                    // term at the previous index is already smaller than 6, the leader-
                    // side optimization discussed above is ineffective. The leader thus
                    // probes at index 8 and, naively, receives a rejection for the same
                    // index and log term 5. Again, the leader optimization does not improve
                    // over linear probing as term 5 is above the leader's term 3 for that
                    // and many preceding indexes; the leader would have to probe linearly
                    // until it would finally hit index 3, where the probe would succeed.
                    //
                    // Instead, we apply a similar optimization on the follower. When the
                    // follower receives the probe at index 8 (log term 3), it concludes
                    // that all of the leader's log preceding that index has log terms of
                    // 3 or below. The largest index in the follower's log with a log term
                    // of 3 or below is index 3. The follower will thus return a rejection
                    // for index=3, log term=3 instead. The leader's next probe will then
                    // succeed at that index.
                    //
                    // [1]: more precisely, if the log terms in the large uncommitted
                    // tail on the follower are larger than the leader's. At first,
                    // it may seem unintuitive that a follower could even have such
                    // a large tail, but it can happen:
                    //
                    // 1. Leader appends (but does not commit) entries 2 and 3, crashes.
                    //   idx        1 2 3 4 5 6 7 8 9
                    //              -----------------
                    //   term (L)   1 2 2     [crashes]
                    //   term (F)   1
                    //   term (F)   1
                    //
                    // 2. a follower becomes leader and appends entries at term 3.
                    //              -----------------
                    //   term (x)   1 2 2     [down]
                    //   term (F)   1 3 3 3 3
                    //   term (F)   1
                    //
                    // 3. term 3 leader goes down, term 2 leader returns as term 4
                    //    leader. It commits the log & entries at term 4.
                    //
                    //              -----------------
                    //   term (L)   1 2 2 2
                    //   term (x)   1 3 3 3 3 [down]
                    //   term (F)   1
                    //              -----------------
                    //   term (L)   1 2 2 2 4 4 4
                    //   term (F)   1 3 3 3 3 [gets probed]
                    //   term (F)   1 2 2 2 4 4 4
                    //
                    // 4. the leader will now probe the returning follower at index
                    //    7, the rejection points it at the end of the follower's log
                    //    which is at a higher log term than the actually committed
                    //    log.
                    nextProbeIdx = log_.findConflictByTerm(m.rejectHint.index, m.rejectHint.term).index;
                }
                if (pr->maybeDecrTo(m.index, nextProbeIdx)) {
                    // logger_.debug("{} decreased progress of {} to [{}]", id_, m.from, pr)
                    if (pr->state() == tracker::StateReplicate) {
                        pr->becomeProbe();
                    }
                    sendAppend(m.from);
                }
            } else {
                auto oldPaused = pr->isPaused();
                // We want to update our tracking if the response updates our
                // matched index or if the response can move a probing peer back
                // into StateReplicate (see heartbeat_rep_recovers_from_probing.txt
                // for an example of the latter case).
                // NB: the same does not make sense for StateSnapshot - if `m.index`
                // equals pr->Match we know we don't m.index+1 in our log, so moving
                // back to replicating state is not useful; besides pr->PendingSnapshot
                // would prevent it.
                if (pr->update(m.index) || (pr->match() == m.index && pr->state() == tracker::StateProbe)) {
                    if (pr->state() == tracker::StateProbe) {
                        pr->becomeReplicate();
                    } else if (pr->state() == tracker::StateSnapshot && pr->match() + 1 >= log_.firstIndex()) {
                        // Note that we don't take into account PendingSnapshot to
                        // enter this branch. No matter at which index a snapshot
                        // was actually applied, as long as this allows catching up
                        // the follower from the log, we will accept it. This gives
                        // systems more flexibility in how they implement snapshots;
                        // see the comments on PendingSnapshot.

                        // logger_.debug("{} recovered from needing snapshot, resumed sending replication messages to
                        // {} [{}]", id_, m.from, pr)
                        //  Transition back to replicating state via probing state
                        //  (which takes the snapshot into account). If we didn't
                        //  move to replicating state, that would only happen with
                        //  the next round of appends (but there may not be a next
                        //  round for a while, exposing an inconsistent RaftStatus).
                        pr->becomeProbe();
                        pr->becomeReplicate();
                    } else if (pr->state() == tracker::StateReplicate) {
                        pr->inflights().freeLE(m.index);
                    }

                    if (maybeCommit()) {
                        // committed index has progressed for the term, so it is safe
                        // to respond to pending read index requests
                        bcastAppend();
                    } else if (oldPaused) {
                        // If we were paused before, this node may be missing the
                        // latest commit index, so send it.
                        sendAppend(m.from);
                    }
                    // We've updated flow control information above, which may
                    // allow us to send multiple (size-limited) in-flight messages
                    // at once (such as when transitioning from probe to
                    // replicate, or when freeTo() covers multiple messages). If
                    // we have more entries to send, send as many messages as we
                    // can (without sending empty messages for the commit index)
                    if (id_ != m.from) {
                        //??
                        sendAppend(m.from, false /* sendif(Empty */);
                    }
                    // Transfer leadership is in progress.
                    if (m.from == leadTransferee_ && pr->match() == log_.lastIndex()) {
                        // logger_.info("{} sent MsgTimeoutNow to {} after received MsgAppendResponse", id_, m.from)
                        sendTimeoutNow(m.from);
                    }
                }
            }
        } else if constexpr (one_of<Msg, HeartbeatResponse>) {
            auto pr = tracker_.getProgress(m.from);
            if (!pr) {
                return {};
            }

            pr->setRecentActive(true);
            pr->resume();

            // NB: if the follower is paused (full Inflights), this will still send an
            // empty append, allowing it to recover from situations in which all the
            // messages that filled up Inflights in the first place were dropped. Note
            // also that the outgoing heartbeat already communicated the commit index.
            //
            // If the follower is fully caught up but also in StateProbe (as can happen
            // if ReportUnreachable was called), we also want to send an append (it will
            // be empty) to allow the follower to transition back to StateReplicate once
            // it responds.
            //
            // Note that StateSnapshot typically satisfies pr->Match < lastIndex, but
            // `pr->Paused()` is always true for StateSnapshot, so sendAppend is a
            // no-op.
            if (pr->match() < log_.lastIndex() || pr->state() == tracker::StateProbe) {
                sendAppend(m.from);
            }

            if (readOnly_.option() != ReadIndexSafe || m.context.empty()) {
                return {};
            }

            if (tracker_.config().voters_.voteResult([&](NodeId id) {
                    auto& votes = readOnly_.recvAck(m.from, m.context);
                    auto iter = votes.find(id);
                    if (iter == votes.end()) {
                        return quorum::VotePending;
                    }
                    return quorum::VoteWon;
                }) != quorum::VoteWon) {
                return {};
            }

            auto rss = readOnly_.advance(m.context);
            for (auto rs : rss) {
                handleReadIndexReady(rs.req, rs.index);
            }
        } else if constexpr (one_of<Msg, TransferLeaderRequest>) {
            auto pr = tracker_.getProgress(m.from);
            if (!pr) {
                return {};
            }
            if (tracker_.config().learners_.contains(m.from)) {
                // logger_.debug("{} is learner. Ignored transferring leadership", id_)
                return {};
            }
            auto leadTransferee = m.from;
            auto lastLeadTransferee = leadTransferee_;
            if (lastLeadTransferee != InvalidId) {
                if (lastLeadTransferee == leadTransferee) {
                    // logger_.info("{} [term {}] transfer leadership to {} is in progress, ignores request to same
                    // node {}", 	id_, term_, leadTransferee, leadTransferee)
                    return {};
                }
                abortLeaderTransfer();
                // logger_.info("{} [term {}] abort previous transferring leadership to {}", id_, term_,
                // lastLeadTransferee)
            }
            if (leadTransferee == id_) {
                // logger_.debug("{} is already leader. Ignored transferring leadership to self", id_)
                return {};
            }
            // Transfer leadership to third party.
            // logger_.info("{} [term {}] starts to transfer leadership to {}", id_, term_, leadTransferee)
            // Transfer leadership should be finished in one electionTimeout, so reset electionElapsed_.
            electionElapsed_ = 0;
            leadTransferee_ = leadTransferee;
            if (pr->match() == log_.lastIndex()) {
                sendTimeoutNow(leadTransferee);
                // logger_.info("{} sends MsgTimeoutNow to {} immediately as {} already has up-to-date log", id_,
                // leadTransferee, leadTransferee)
            } else {
                sendAppend(leadTransferee);
            }
        }
        return {};
    }

    // stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
    // whether they respond to MsgRequestVoteResponse or MsgRequestPreVoteResponse.
    template <Message Msg>
    Result<> stepCandidate(Msg&& m)
    {
        if constexpr (one_of<Msg, ProposalRequst>) {
            logger_.info("no leader at term {}; dropping proposal", term_);
            return ErrProposalDropped;
        } else if constexpr (one_of<Msg, HeartbeatRequest>) {
            becomeFollower(m.term, m.from);
            handleHeartbeat(m);
        } else if constexpr (one_of<Msg, AppendEntriesRequest>) {
            becomeFollower(m.term, m.from);
            handleAppendEntries(m);
        } else if constexpr (one_of<Msg, InstallSnapshotRequest>) {
            becomeFollower(m.term, m.from); // always m.term == term_
            handleSnapshot(m);
        } else if constexpr (one_of<Msg, VoteResponse>) {
            // Only handle vote responses corresponding to our candidacy (while in
            // StateCandidate, we may get stale MsgRequestPreVoteResponse messages in this term from
            // our pre-candidate state).
            if ((m.pre ? PreCandidate : Candidate) == state_) {
                tracker_.recordVote(m.from, !m.reject);
                auto voteResult = tracker_.voteResult();
                if (voteResult == quorum::VoteWon) {
                    if (state_ == PreCandidate) {
                        campaign(CampaignElection);
                    } else {
                        becomeLeader();
                        bcastAppend();
                    }
                } else if (voteResult == quorum::VoteLost) {
                    becomeFollower(term_, InvalidId);
                }
            }
        }
        return {};
    }

    template <Message Msg>
    Result<> stepFollower(Msg&& m)
    {
        if constexpr (one_of<Msg, ProposalRequst>) {
            if (lead_ == InvalidId) {
                logger_.info("{} no leader at term {}; dropping proposal", id_, term_);
                return ErrProposalDropped;
            } else if (disableProposalForwarding_) {
                logger_.info("{} not forwarding to leader {} at term {}; dropping proposal", id_, lead_, term_);
                return ErrProposalDropped;
            }
            m.to = lead_;
            send(m);
            logger_.debug("forward proposal {} -> {}", id_, lead_);
        } else if constexpr (one_of<Msg, HeartbeatRequest>) {
            electionElapsed_ = 0;
            lead_ = m.from;
            handleHeartbeat(m);
        } else if constexpr (one_of<Msg, AppendEntriesRequest>) {
            electionElapsed_ = 0;
            lead_ = m.from;
            handleAppendEntries(m);
        } else if constexpr (one_of<Msg, InstallSnapshotRequest>) {
            electionElapsed_ = 0;
            lead_ = m.from;
            handleSnapshot(m);
        } else if constexpr (one_of<Msg, TransferLeaderRequest>) {
            if (lead_ == InvalidId) {
                logger_.info("{} no leader at term {}; dropping leader transfer msg", id_, term_);
                return {};
            }
            m.to = lead_;
            send(m);
        } else if constexpr (one_of<Msg, TimeoutNowRequest>) {
            logger_.info("{} [term {}] received MsgTimeoutNow from {} and starts an election to get leadership.", id_,
                         term_, m.from);
            //  Leadership transfers never use pre-vote even if r.preVote is true; we
            //  know we are not recovering from a partition so there is no need for the
            //  extra round trip.
            hup(true);
        } else if constexpr (one_of<Msg, ReadIndexRequest>) {
            if (lead_ == InvalidId) {
                logger_.debug("no leader at term {}; dropping index reading msg", term_);
                return {};
            }
            m.to = lead_;
            send(m);
        } else if constexpr (one_of<Msg, ReadIndexResponse>) {
            readStates_.push_back(ReadState{ m.index, m.context });
        }
        return {};
    }

    // restore recovers the state machine from a snapshot. It restores the log and the
    // configuration of state machine. If this method returns false, the snapshot was
    // ignored, either because it was obsolete or because of an error.
    bool restore(SnapshotPtr snapshot)
    {
        if (snapshot->meta.index <= log_.committed_) {
            return false;
        }

        if (state_ != Follower) {
            // This is defense-in-depth: if the leader somehow ended up applying a
            // snapshot, it could move into a new term without moving into a
            // follower state. This should never fire, but if it did, we'd have
            // prevented damage by returning early, so log only a loud warning.
            //
            // At the time of writing, the instance is guaranteed to be in follower
            // state when this method is called.
            // r.logger.Warningf("{} attempted to restore snapshot as leader; should never happen", id_)
            becomeFollower(term_ + 1, InvalidId);
            return false;
        }

        // More defense-in-depth: throw away snapshot if recipient is not in the
        // config. This shouldn't ever happen (at the time of writing) but lots of
        // code here and there assumes that id_ is in the progress tracker::
        auto& cs = snapshot->meta.confState;

        auto contains = [](auto& c, auto& v) { return std::binary_search(c.begin(), c.end(), v); };

        bool found = contains(cs.voters, id_) || contains(cs.learners, id_) || contains(cs.votersOutgoing, id_);

        if (!found) {
            return false;
        }

        // Now go ahead and actually restore.

        if (log_.matchTerm(snapshot->meta.index, snapshot->meta.term)) {
            // logger_.info("{} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to snapshot [index:
            // {}, term: {}]", 	id_, log_.committed, log_.lastIndex(), log_.lastTerm(), s.meta.Index,
            // s.meta.Term)
            log_.commitTo(snapshot->meta.index);
            return false;
        }

        log_.restore(snapshot);

        // Reset the configuration and add the (potentially updated) peers in anew.
        // tracker_ = std::make_shared<tracker::ProgressTracker>(tracker_.maxInflight_, tracker_.maxInflightBytes_);

        // panic should never happen. Either there's a bug in our config change
        // handling or the client corrupted the conf change.
        auto changeResult = std::move(confchange::restore(cs, tracker_, log_.lastIndex()).unwrap());

        if (cs != switchToConfig(changeResult)) {
            panic("mismatching confstate after snapshot restore");
        }

        auto pr = tracker_.getProgress(id_);
        pr->update(pr->next() - 1); // TODO(tbg): this is untested and likely unneeded

        // logger_.info("{} [commit: {}, lastindex: {}, lastterm: {}] restored snapshot [index: {}, term: {}]",
        //	id_, log_.committed, log_.lastIndex(), log_.lastTerm(), s.meta.Index, s.meta.Term)
        return true;
    }

    // promotable indicates whether state machine can be promoted to leader,
    // which is true when its own id is in progress list.
    bool promotable()
    {
        return tracker_.contains(id_) && !tracker_.isLearner(id_) && !log_.hasNextOrInProgressSnapshot();
    }

    void loadState(const HardState& state)
    {
        if (state.commit < log_.committed_ || state.commit > log_.lastIndex()) {
            panic("{} state.commit {} is out of range [{}, {}]");
        }
        log_.committed_ = state.commit;
        term_ = state.term;
        vote_ = state.vote;
    }

    // pastElectionTimeout returns true if electionElapsed_ is greater
    // than or equal to the randomized election timeout in
    // [electiontimeout, 2 * electiontimeout - 1].
    bool pastElectionTimeout() { return electionElapsed_ >= randomizedElectionTimeout_; }

    void sendTimeoutNow(NodeId to)
    {
        // Message msg;
        // msg.to = to;
        // msg.type = MsgTimeoutNow;
        // send(std::move(msg));
    }

    void abortLeaderTransfer() { leadTransferee_ = InvalidId; }

    // committedEntryInCurrentTerm return true if the peer has committed an entry in its term.
    bool committedEntryInCurrentTerm()
    {
        // NB: term_ is never 0 on a leader, so if zeroTermOnOutOfBounds returns 0,
        // we won't see it as a match with term_.
        return log_.term(log_.committed_).value_or(0) == term_;
    }

    // responseToReadIndex constructs a response for `req`. If `req` comes from the peer
    // itself, a blank value will be returned.
    void handleReadIndexReady(ReadIndexRequest& m, Index idx)
    {
        if (m.from == InvalidId || m.from == id_) {
            readStates_.push_back(ReadState{ idx, m.context });
            return;
        }

        send(ReadIndexResponse{
          .to = m.from,
          .index = idx,
          .context = m.context,
        });
    }

    // increaseUncommittedSize computes the size of the proposed entries and
    // determines whether they would push leader over its maxUncommittedSize limit.
    // If the new entries would exceed the limit, the method returns false. If not,
    // the increase in uncommitted entry size is recorded and the method returns
    // true.
    //
    // Empty payloads are never refused. This is used both for appending an empty
    // entry at a new leader's term, as well as leaving a joint configuration.
    bool increaseUncommittedSize(EntrySlice ents)
    {
        auto s = payloadSize(ents);
        if (uncommittedSize_ > 0 && s > 0 && uncommittedSize_ + s > maxUncommittedSize_) {
            // If the uncommitted tail of the Raft log is empty, allow any size
            // proposal. Otherwise, limit the size of the uncommitted tail of the
            // log and drop any proposal that would push the size over the limit.
            // Note the added requirement s>0 which is used to make sure that
            // appending single empty entries to the log always succeeds, used both
            // for replicating a new leader's initial empty entry, and for
            // auto-leaving joint configurations.
            return false;
        }
        uncommittedSize_ += s;
        return true;
    }

    // reduceUncommittedSize accounts for the newly committed entries by decreasing
    // the uncommitted entry size limit.
    void reduceUncommittedSize(size_t size)
    {
        if (size > uncommittedSize_) {
            // uncommittedSize may underestimate the size of the uncommitted Raft
            // log tail but will never overestimate it. Saturate at 0 instead of
            // allowing overflow.
            uncommittedSize_ = 0;
        } else {
            uncommittedSize_ -= size;
        }
    }

    void advanceAppend(Term term, Index snapshotIndex, Index index, Term lastTerm)
    {
        if (snapshotIndex != 0) {
            log_.stableSnapshot(snapshotIndex);
            appliedTo(snapshotIndex, 0);
        }

        if (term < term_) {
            return;
        }

        if (index != 0) {
            log_.stableEntries(index, lastTerm);
        }
    }

    void unreachable(NodeId id)
    {
        assert(state_ == Leader);
        auto pr = tracker_.getProgress(id);
        if (pr) {
            // During optimistic replication, if the remote becomes unreachable,
            // there is huge probability that a MsgAppend is lost.
            if (pr->state() == tracker::StateReplicate) {
                pr->becomeProbe();
            }
        }
    }

    void snapshotStatus(NodeId id, bool reject)
    {
        assert(state_ == Leader);
        auto pr = tracker_.getProgress(id);
        if (!pr) {
            return;
        }

        if (pr->state() != tracker::StateSnapshot) {
            return;
        }
        if (reject) {
            // NB: the order here matters or we'll be probing erroneously from
            // the snapshot index, but the snapshot never applied.
            pr->pendingSnapshot_ = 0;
        }
        pr->becomeProbe();

        // If snapshot finish, wait for the MsgAppendResponse from the remote node before sending
        // out the next MsgAppend.
        // If snapshot failure, wait for a heartbeat interval before next try
        pr->pause();
    }

    void checkQuorum()
    {
        assert(state_ == Leader);

        if (!tracker_.quorumActive()) {
            becomeFollower(term_, InvalidId);
        }

        // Mark everyone (but ourselves) as inactive in preparation for the next
        // CheckQuorum.
        tracker_.visit([this](NodeId id, tracker::Progress& pr) {
            if (id != id_) {
                pr.setRecentActive(false);
            }
        });
    }

    void handleHeartbeat(HeartbeatRequest& m)
    {
        log_.commitTo(m.commit);
        send(HeartbeatResponse{
          .to = m.from,
          .context = m.context,
        });
        return;
    }

    void handleAppendEntries(AppendEntriesRequest& m)
    {

        if (m.prevLog.index < log_.committed_) {
            send(AppendEntriesResponse{
              .to = m.from,
              .index = log_.committed_,
              .reject = false,
            });
            return;
        }

        auto lastIndex = log_.maybeAppend(m.prevLog.index, m.prevLog.term, m.commit, m.entries);
        if (lastIndex) {
            send(AppendEntriesResponse{
              .to = m.from,
              .index = *lastIndex,
              .reject = false,
            });
            return;
        }

        // Our log does not match the leader's at index m.index. Return a hint to the
        // leader - a guess on the maximal (index, term) at which the logs match. Do
        // this by searching through the follower's log for the maximum (index, term)
        // pair with a term <= the MsgAppend's LogTerm and an index <= the MsgAppend's
        // Index. This can help skip all indexes in the follower's uncommitted tail
        // with terms greater than the MsgAppend's LogTerm.
        //
        // See the other caller for findConflictByTerm (in stepLeader) for a much more
        // detailed explanation of this mechanism.

        // NB: m.index >= raftLog.committed by now (see the early return above), and
        // raftLog.lastIndex() >= raftLog.committed by invariant, so min of the two is
        // also >= raftLog.committed. Hence, the findConflictByTerm argument is within
        // the valid interval, which then will return a valid (index, term) pair with
        // a non-zero term (unless the log is empty). However, it is safe to send a zero
        // LogTerm in this response in any case, so we don't verify it here.
        auto hint = log_.findConflictByTerm(std::min(m.prevLog.index, log_.lastIndex()), m.prevLog.term);

        send(AppendEntriesResponse{
          .to = m.from,
          .index = m.prevLog.index,
          .reject = true,
          .rejectHint = hint,
        });
    }

    void handleSnapshot(InstallSnapshotRequest& m)
    {
        // MsgSnapshot messages should always carry a non-nil Snapshot, but err on the
        // side of safety and treat a nil Snapshot as a zero-valued Snapshot.
        auto snapshot = std::make_shared<Snapshot>(m.snapshot);

        auto sindex = snapshot->meta.index;
        auto sterm = snapshot->meta.term;

        InstallSnapshotResponse resp;
        resp.to = m.from;

        if (restore(snapshot)) {
            logger_.info("{} [commit: {}] restored snapshot [index: {}, term: {}]", id_, log_.committed_, sindex,
                         sterm);
            resp.index = log_.lastIndex();
        } else {
            logger_.info("{} [commit: {}] ignored snapshot [index: {}, term: {}]", id_, log_.committed_, sindex, sterm);
            resp.index = log_.committed_;
        }
        send(resp);
    }

    void handleReadIndex(ReadIndexRequest& m)
    {
        if (state_ != Leader) {
            return;
        }

        if (tracker_.isSingleton()) {
            handleReadIndexReady(m, log_.committed_);
            return;
        }

        // Reject read only request when this leader has not committed any log entry
        // in its term.
        if (!committedEntryInCurrentTerm()) {
            return;
        }

        // thinking: use an internally defined context instead of the user given context.
        // We can express this in terms of the term and index instead of a user-supplied value.
        // This would allow multiple reads to piggyback on the same message.
        switch (readOnly_.option()) {
        // If more than the local vote is needed, go through a full broadcast.
        case ReadIndexSafe:
            readOnly_.addRequest(log_.committed_, m);
            // The local node automatically acks the request.
            readOnly_.recvAck(id_, m.context);
            bcastHeartbeatWithCtx(m.context);
            break;
        case ReadIndexLeaseBased:
            handleReadIndexReady(m, log_.committed_);
            break;
        }
    }

private:
    friend class Node<T>;

    Logger& logger_;

    NodeId id_;

    Term term_;

    NodeId vote_;

    StateType state_;

    // the log
    Log<T> log_;

    tracker::ProgressTracker tracker_;

    std::vector<ReadState> readStates_;

    // isLearner is true if the local raft node is a learner.
    bool isLearner_;

    // msgs contains the list of messages that should be sent out immediately to
    // other nodes.  Messages in this list must target other nodes.
    std::vector<MessageHolder> msgs_;

    ReadOnly readOnly_;

    // the leader id
    NodeId lead_;
    // leadTransferee is id of the leader transfer target when its value is not zero.
    // Follow the procedure defined in raft thesis 3.10.
    NodeId leadTransferee_;
    // Only one conf change may be pending (in the log, but not yet
    // applied) at a time. This is enforced via pendingConfIndex, which
    // is set to a value >= the log index of the latest pending
    // configuration change (if any). Config changes are only allowed to
    // be proposed if the leader's applied index is greater than this
    // value.
    NodeId pendingConfIndex_;
    // disableConfChangeValidation is Config.DisableConfChangeValidation,
    // see there for details.
    bool disableConfChangeValidation_;

    size_t maxMsgSize_;

    size_t maxUncommittedSize_;

    // an estimate of the size of the uncommitted tail of the Raft log. Used to
    // prevent unbounded log growth. Only maintained by the leader. Reset on
    // term changes.
    size_t uncommittedSize_;

    // number of ticks since it reached last electionTimeout when it is leader
    // or candidate.
    // number of ticks since it reached last electionTimeout or received a
    // valid message from current leader when it is a follower.
    size_t electionElapsed_;

    // number of ticks since it reached last heartbeatTimeout.
    // only leader keeps heartbeatElapsed.
    size_t heartbeatElapsed_;

    bool checkQuorum_;
    bool preVote_;

    size_t heartbeatTimeout_;
    size_t electionTimeout_;

    // randomizedElectionTimeout is a random number between
    // [electiontimeout, 2 * electiontimeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    int randomizedElectionTimeout_;
    bool disableProposalForwarding_;
    bool stepDownOnRemoval_;

    RandomGenerator<size_t> rng_;
};

} // namespace raft

template <typename CharT>
struct std::formatter<raft::StateType, CharT> : std::formatter<string_view, CharT>
{
    template <class FormatContext>
    auto format(raft::StateType s, FormatContext& ctx) const
    {
        static const std::array<std::string_view, 5> stamp = { "Follower", "Candidate", "Leader", "PreCandidate",
                                                               "InvalidState" };

        if (s > raft::NumStates) {
            s = raft::NumStates;
        }
        return std::format_to(ctx.out(), "[{}]", stamp[s]);
    }
};
