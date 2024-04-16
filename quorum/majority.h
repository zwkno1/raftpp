#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <quorum/quorum.h>

namespace raft {
namespace quorum {

// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
class MajorityConfig
{
public:
    MajorityConfig(std::set<uint64_t> ids = {})
      : ids_(std::move(ids))
    {
    }

    // Describe returns a (multi-line) representation of the commit indexes for the
    // given lookuper.
    template <AckedIndexer T>
    std::string describe(const T& l)
    {
        if (ids_.empty()) {
            return "<empty majority quorum>";
        }

        struct tup
        {
            uint64_t id;
            Index idx;
            bool ok;
            int bar; // length of bar displayed for this tup
        };

        // Below, populate .bar so that the i-th largest commit index has bar i (we
        // plot this as sort of a progress bar). The actual code is a bit more
        // complicated and also makes sure that equal index => equal bar.

        auto n = ids_.size();

        std::vector<tup> info;
        info.reserve(n);
        for (auto id : ids_) {
            auto idx = l.ackedIndex(id);
            info.push_back(tup{ id, idx ? *idx : 0, idx.has_value(), 0 });
        }

        // Sort by index
        std::sort(info.begin(), info.end(), [](const tup& l, const tup& r) {
            if (l.idx == r.idx) {
                return l.id < r.id;
            }
            return l.idx < r.idx;
        });

        // Populate .bar.
        for (auto i = 0; i < info.size(); ++i) {
            if (i > 0 && info[i - 1].idx < info[i].idx) {
                info[i].bar = i;
            }
        }

        // Sort by ID.
        std::sort(info.begin(), info.end(), [](const tup& l, const tup& r) { return l.id < r.id; });

        std::stringstream ss;

        // Print.
        ss << std::string(n, ' ') << "    idx\n";

        for (auto& i : info) {
            if (!i.ok) {
                ss << "?" << std::string(n, ' ');
            } else {
                ss << std::string(i.bar, 'x') << ">" << std::string(n - i.bar, ' ');
            }
            ss << " " << std::setw(5) << i.idx << std::setw(0) << "    (id=" << i.id << ")\n";
        }
        return ss.str();
    }

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

        std::array<uint64_t, 16> arr;
        std::vector<uint64_t> vec;

        uint64_t* idxs = arr.data();
        int n = 0;

        if (ids_.size() > arr.size()) {
            vec.resize(ids_.size());
            idxs = vec.data();
        }

        for (auto id : ids_) {
            auto idx = l.ackedIndex(id);
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
    VoteResult voteResult(const std::unordered_map<uint64_t, bool>& votes) const
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
            auto iter = votes.find(id);
            if (iter == votes.end()) {
                ++missing;
                continue;
            }

            if (iter->second) {
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

    inline void add(uint64_t id) { ids_.insert(id); }

    inline void remove(uint64_t id) { ids_.erase(id); }

    inline void clear() { ids_.clear(); }

    inline const std::set<uint64_t>& ids() const { return ids_; }

    inline bool contains(uint64_t id) const { return ids_.contains(id); }

    inline bool empty() const { return ids_.empty(); }

    inline std::string str() const
    {
        std::stringstream ss;
        ss << "(";
        for (auto iter = ids_.begin(); iter != ids_.end(); ++iter) {
            if (iter != ids_.begin()) {
                ss << " ";
            }
            ss << *iter;
        }
        ss << ")";
        return ss.str();
    }

private:
    std::set<uint64_t> ids_;
};

} // namespace quorum
} // namespace raft