#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include <raft.h>
#include <readindex.h>
#include <result.h>
#include <utils.h>

#include <raftpb/raft.pb.h>

namespace raft {

struct Ready
{
    Ready()
      : mustSync_(false)
    {
    }

    std::optional<SoftState> softState_;

    std::optional<pb::HardState> hardState_;

    std::vector<ReadState> readStates_;

    EntryList entries_;

    SnapshotPtr snapshot_;

    EntryList committedEntries_;

    std::vector<pb::Message> msgs_;
    std::vector<pb::Message> msgsAfterAppend_;

    bool mustSync_;
};

// struct ReadyRecord
//{
//     uint64_t number;
//     std::optional<IndexTerm> lastEntry;
//     std::optional<IndexTerm> snapshot;
// };
//
// struct LightReady
//{
//     std::optional<Index> committedIndex;
//     EntryList committedEntries;
//     std::vector<pb::Message> messages;
// };

// ErrStepLocalMsg is returned when try to step a local raft message
// var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.trk for that node.
// var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// RawNode is a thread-unsafe Node.
// The methods of this struct correspond to the methods of Node and are described
// more fully there.

struct Peer
{
    uint64_t id;
    std::string context;
};

template <Storage T>
class Node
{
public:
    // NewRawNode instantiates a RawNode from the given configuration.
    //
    // See Bootstrap() for bootstrapping an initial state; this replaces the former
    // 'peers' argument to this method (with identical behavior). However, It is
    // recommended that instead of calling Bootstrap, applications bootstrap their
    // state manually by setting up a Storage that has a first index > 1 and which
    // stores the desired ConfState as its InitialState.
    Node(Config& config, T& storage)
      : raft_(config, storage)
    {
        prevHardState_ = raft_.hardState();
        prevSoftState_ = raft_.softState();
    }

    // Tick advances the internal logical clock by a single tick.
    void tick() { raft_.tick(); }

    // Step advances the state machine using the given message.
    Result<> step(pb::Message& m)
    {
        // Ignore unexpected local messages receiving over network.
        if (isResponseMsg(m.type()) && !raft_.tracker_.contains(m.from())) {
            return ErrStepPeerNotFound;
        }
        return raft_.step(m);
    }

    // campaign causes this RawNode to transition to candidate state.
    Result<> campaign() { return raft_.hup(); }

    // Propose proposes data be appended to the raft log.
    Result<> propose(std::string data)
    {
        pb::Message msg;
        msg.set_type(pb::MsgPropose);
        msg.set_from(raft_.id());
        msg.add_entries()->set_data(std::move(data));
        return raft_.step(msg);
    }

    // ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
    // details.
    Result<> proposeConfChange(pb::ConfChange cc)
    {
        auto msg = confChangeToMsg(cc);
        return raft_.step(msg);
    }

    // ApplyConfChange applies a config change to the local node. The app must call
    // this when it applies a configuration change, except when it decides to reject
    // the configuration change, in which case no call must take place.
    pb::ConfState applyConfChange(pb::ConfChange cc) { return raft_.applyConfChange(cc); }

    // Ready returns the outstanding work that the application needs to handle. This
    // includes appending and applying entries or a snapshot, updating the HardState,
    // and sending messages. The returned Ready() *must* be handled and subsequently
    // passed back via Advance().
    std::shared_ptr<const Ready> ready()
    {
        auto& log = raft_.log_;

        if (ready_) {
            panic("call ready twice without call to advance");
        }

        if (!hasReady()) {
            return ready_;
        }

        ready_ = std::make_shared<Ready>();
        auto& rd = *ready_;

        rd.entries_ = log.nextUnstableEntries();
        rd.committedEntries_ = log.nextCommittedEntries(true);
        rd.msgs_ = std::move(raft_.msgs_);
        rd.msgsAfterAppend_ = std::move(raft_.msgsAfterAppend_);
        for (auto& msg : rd.msgsAfterAppend_) {
            if (msg.to() != raft_.id()) {
                rd.msgs_.push_back(msg);
            }
        }

        auto ss = raft_.softState();
        if (ss != prevSoftState_) {
            rd.softState_ = ss;
            prevSoftState_ = ss;
        }

        auto hs = raft_.hardState();
        if (hs != prevHardState_) {
            rd.hardState_ = hs;
            prevHardState_ = hs;
        }

        if (log.hasNextUnstableSnapshot()) {
            rd.snapshot_ = log.nextUnstableSnapshot();
        }

        rd.readStates_ = raft_.readStates_;
        rd.mustSync_ =
          (hs.term() != prevHardState_.term() || hs.vote() != prevHardState_.vote() || !rd.entries_.empty());

        log.acceptUnstable();
        if (!rd.committedEntries_.empty()) {
            auto index = rd.committedEntries_.back().index();
            log.acceptApplying(index, entsSize(rd.committedEntries_), true);
        }

        return ready_;
    }

    void advance()
    {
        if (!ready_) {
            return;
        }

        auto& log = raft_.log_;

        auto& rd = *ready_;

        for (auto& msg : rd.msgsAfterAppend_) {
            if (msg.to() == raft_.id()) {
                raft_.step(msg);
            }
        }

        if (log.hasNextOrInProgressUnstableEnts()) {
            log.stableEntries(log.lastIndex(), log.lastTerm());
        }

        if (rd.snapshot_) {
            raft_.appliedSnapshot(rd.snapshot_->metadata().index());
        }

        if (!rd.entries_.empty() || rd.snapshot_) {
            auto index = log.lastIndex();
            auto term = log.lastTerm();

            log.stableEntries(index, term);

            if (rd.snapshot_) {
                raft_.appliedSnapshot(rd.snapshot_->metadata().index());
            }
        }

        if (!rd.committedEntries_.empty()) {
            auto index = rd.committedEntries_.back().index();
            raft_.appliedTo(index, entsSize(rd.committedEntries_));
            raft_.reduceUncommittedSize(payloadsSize(rd.committedEntries_));
        }

        ready_ = nullptr;
    }

    void bootstrap(const std::vector<Peer>& peers)
    {
        if (peers.empty()) {
            panic("must provide at least one peer to Bootstrap");
        }

        auto lastIndex = raft_.log_.storage().lastIndex().unwrap();
        if (lastIndex != 0) {
            return panic("can't bootstrap a nonempty Storage");
        }

        // We've faked out initial entries above, but nothing has been
        // persisted. Start with an empty HardState (thus the first Ready will
        // emit a HardState update for the app to persist).
        // rn.prevHardSt = emptyState

        raft_.becomeFollower(1, 0);

        google::protobuf::RepeatedPtrField<raft::pb::Entry> entries;
        for (size_t i = 0; i < peers.size(); i++) {
            auto& peer = peers[i];
            pb::ConfChange cc;
            auto cs = cc.add_changes();
            cs->set_type(pb::AddNode);
            cs->set_node_id(peer.id);
            pb::Entry e;
            e.set_type(pb::EntryConfChange);
            e.set_term(1);
            e.set_index(i + 1);
            e.set_data(cc.SerializeAsString());
            entries.Add(std::move(e));
        }
        raft_.log_.append(EntrySlice{ entries });

        // Now apply them, mainly so that the application can call Campaign
        // immediately after StartNode in tests. Note that these nodes will
        // be added to raft twice: here and when the application's Ready
        // loop calls ApplyConfChange. The calls to addNode must come after
        // all calls to raftLog.append so progress.next is set after these
        // bootstrapping entries (it is an error if we try to append these
        // entries since they have already been committed).
        // We do not set raftLog.applied so the application will be able
        // to observe all conf changes via Ready.CommittedEntries.
        //
        // TODO(bdarnell): These entries are still unstable; do we need to preserve
        // the invariant that committed < unstable?
        raft_.log_.committed_ = entries.size();
        for (auto& peer : peers) {
            pb::ConfChange cc;
            auto cs = cc.add_changes();
            cs->set_type(pb::AddNode);
            cs->set_node_id(peer.id);

            raft_.applyConfChange(cc);
        }
    }

    uint64_t leaderId() { return prevSoftState_.leaderId_; }

private:
    bool mustSync(pb::HardState st, pb::HardState prevst, int entsnum)
    {
        // Persistent state on all servers:
        // (Updated on stable storage before responding to RPCs)
        // currentTerm
        // votedFor
        // log entries[]
        return (entsnum != 0 || st.vote() != prevst.vote() || st.term() != prevst.term());
    }

    bool hasReady()
    {
        auto& log = raft_.log_;

        if (raft_.softState() != prevSoftState_) {
            return true;
        }

        if (raft_.hardState() != prevHardState_) {
            return true;
        }

        if (!raft_.readStates_.empty()) {
            return true;
        }

        if (!raft_.msgs_.empty()) {
            return true;
        }

        if (!raft_.msgsAfterAppend_.empty()) {
            return true;
        }

        if (log.hasNextUnstableSnapshot()) {
            return true;
        }

        if (log.hasNextUnstableEntries() || log.hasNextCommittedEntries(true)) {
            return true;
        }

        return false;
    }

    Raft<T> raft_;

    SoftState prevSoftState_;
    pb::HardState prevHardState_;
    std::shared_ptr<Ready> ready_;
};

} // namespace raft