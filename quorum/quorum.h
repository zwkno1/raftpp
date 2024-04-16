#pragma once

#include <cstdint>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>

#include <result.h>
#include <utils.h>

namespace raft {
namespace quorum {

// VoteResult indicates the outcome of a vote.
enum VoteResult : std::uint8_t
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
concept AckedIndexer = requires(const T t) {
    {
        t.ackedIndex(uint64_t{})
    } -> std::same_as<std::optional<Index>>;
};

class MapAckIndexer
{
public:
    std::optional<Index> ackedIndex(uint64_t id) const
    {
        auto iter = idxs_.find(id);
        if (iter != idxs_.end()) {
            return iter->second;
        }
        return {};
    }

    void add(uint64_t id, Index idx) { idxs_[id] = idx; }

    std::string str() const
    {
        std::stringstream ss;
        ss << "(";
        for (auto iter = idxs_.begin(); iter != idxs_.end(); ++iter) {
            if (iter != idxs_.begin()) {
                ss << " ";
            }
            ss << iter->first << "," << iter->second;
        }
        ss << ")";
        return ss.str();
    }

private:
    std::unordered_map<uint64_t, Index> idxs_;
};

} // namespace quorum
} // namespace raft