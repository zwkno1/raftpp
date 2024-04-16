
#include <algorithm>
#include <limits>
#include <map>
#include <ostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <quorum/majority.h>
#include <quorum/quorum.h>
#include <utils.h>

using namespace raft;
using namespace raft::quorum;

std::mt19937 rng(std::random_device{}());

// smallRandIdxMap returns a reasonably sized map of ids to commit indexes.
std::map<uint64_t, Index> SmallRandIdxMap()
{
    // Hard-code a reasonably small size here (quick will hard-code 50, which
    // is not useful here).
    auto size = 10;

    auto n = rng() % size;
    std::vector<uint64_t> ids;
    for (int i = 0; i < n * 2; i++) {
        ids.push_back(i);
    }
    std::shuffle(ids.begin(), ids.end(), rng);

    std::vector<uint64_t> idx;
    for (int i = 0; i < n; i++) {
        idx.push_back(rand() % n);
    }

    std::map<uint64_t, Index> m;

    for (int i = 0; i < n; i++) {
        m[ids[i]] = idx[i];
    }
    return m;
}

// This is an alternative implementation of (MajorityConfig).CommittedIndex(l).
Index AlternativeMajorityCommittedIndex(MajorityConfig& c, MapAckIndexer& l)
{
    if (c.ids().empty()) {
        return IndexMax;
    }

    std::map<uint64_t, Index> idToIdx;
    for (auto id : c.ids()) {
        auto idx = l.ackedIndex(id);
        if (idx) {
            idToIdx[id] = *idx;
        }
    }

    // Build a map from index to voters who have acked that or any higher index.
    std::map<Index, int> idxToVotes;
    for (auto& i : idToIdx) {
        idxToVotes[i.second] = 0;
    }

    for (auto [_, idx] : idToIdx) {
        for (auto [idy, _] : idxToVotes) {
            if (idy > idx) {
                continue;
            }
            idxToVotes[idy]++;
        }
    }

    // Find the maximum index that has achieved quorum.
    auto q = c.ids().size() / 2 + 1;
    Index maxQuorumIdx = 0;
    for (auto [idx, n] : idxToVotes) {
        if (n >= q && idx > maxQuorumIdx) {
            maxQuorumIdx = idx;
        }
    }

    return maxQuorumIdx;
}

std::string to_string(const std::map<uint64_t, Index>& m)
{
    std::stringstream os;
    os << "{";
    for (auto& i : m) {
        os << i.first << ": " << i.second << ", ";
    }
    os << "}";
    return os.str();
}

TEST(Quorum, committedIndex)
{
    MajorityConfig c;
    MapAckIndexer l;
    auto m1 = SmallRandIdxMap();
    auto m2 = SmallRandIdxMap();
    for (auto i : m1) {
        c.add(i.first);
    }
    for (auto i : m2) {
        l.add(i.first, i.second);
    }

    EXPECT_EQ(c.committedIndex(l), AlternativeMajorityCommittedIndex(c, l)) << to_string(m1) << " <> " << to_string(m2);
}
