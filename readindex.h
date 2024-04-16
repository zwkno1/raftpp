#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include <utils.h>

#include <raftpb/raft.pb.h>

namespace raft {

enum ReadIndexOption : uint32_t
{
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    ReadIndexSafe = 0,
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    ReadIndexLeaseBased,
};

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
struct ReadState
{
    uint64_t index;
    std::string context;
};

struct ReadIndexStatus
{
    pb::Message req;
    Index index;
    // NB: this never records 'false', but it's more convenient to use this
    // instead of a map[uint64]struct{} due to the API of quorum.VoteResult. If
    // this becomes performance sensitive enough (doubtful), quorum.VoteResult
    // can change to an API that is closer to that of CommittedIndex.
    std::unordered_map<uint64_t, bool> acks;
};

struct ReadIndexContext
{
public:
    ReadIndexContext(ReadIndexOption opt)
      : option_(opt)
    {
    }

    // addRequest adds a read only request into readonly struct.
    // `index` is the commit index of the raft state machine when it received
    // the read only request.
    // `m` is the original read only request message from the local or remote node.
    inline void addRequest(Index index, const pb::Message& m)
    {
        auto& s = m.entries(0).data();
        if (readIndexStatus_.contains(s)) {
            return;
        }
        readIndexStatus_.emplace(s, ReadIndexStatus{ m, index });
        readIndexQueue_.push_back(s);
    }

    // recvAck notifies the readonly struct that the raft state machine received
    // an acknowledgment of the heartbeat that attached with the read only request
    // context.
    const std::unordered_map<Index, bool>& recvAck(uint64_t id, const std::string& context)
    {
        auto iter = readIndexStatus_.find(context);

        if (iter == readIndexStatus_.end()) {
            const static std::unordered_map<Index, bool> empty;
            return empty;
        }
        iter->second.acks[id] = true;

        return iter->second.acks;
    }

    // advance advances the read only request queue kept by the readonly struct.
    // It dequeues the requests until it finds the read only request that has
    // the same context as the given `m`.
    std::vector<ReadIndexStatus> advance(const std::string& context)
    {
        std::vector<ReadIndexStatus> rss;
        auto iter = std::find(readIndexQueue_.begin(), readIndexQueue_.end(), context);

        // found
        if (iter == readIndexQueue_.end()) {
            return rss;
        }
        ++iter;

        rss.reserve(iter - readIndexQueue_.begin());

        for (auto& ctx : std::ranges::subrange(readIndexQueue_.begin(), iter)) {
            auto it = readIndexStatus_.find(ctx);
            rss.emplace_back(std::move(it->second));
            readIndexStatus_.erase(it);
        }

        readIndexQueue_.erase(readIndexQueue_.begin(), iter);

        return rss;
    }

    // lastPendingRequestCtx returns the context of the last pending read only
    // request in readonly struct.
    inline const std::string& lastPendingRequestCtx() const
    {
        if (readIndexQueue_.empty()) {
            const static std::string empty;
            return empty;
        }
        return readIndexQueue_.back();
    }

    inline ReadIndexOption option() const { return option_; }

    inline void reset()
    {
        readIndexStatus_.clear();
        readIndexQueue_.clear();
    }

private:
    ReadIndexOption option_;
    std::unordered_map<std::string, ReadIndexStatus> readIndexStatus_;
    std::vector<std::string> readIndexQueue_;
};

} // namespace raft