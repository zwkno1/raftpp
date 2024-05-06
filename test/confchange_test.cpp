#include <algorithm>
#include <cstddef>
#include <iterator>
#include <random>
#include <vector>

#include <gtest/gtest.h>
#include <raftpp/raftpp.h>

#include "raftpp/detail/message.h"
#include "raftpp/detail/utils.h"

std::mt19937 rng(std::random_device{}());

using namespace raft;

// Generate creates a random (valid) ConfState for use with quickcheck.
raft::ConfState generate()
{
    raft::ConfState cs;
    // NB: never generate the empty ConfState, that one should be unit tested.
    auto nVoters = rng() % 5 + 1;

    auto nLearners = rng() % 5;
    // The number of voters that are in the outgoing config but not in the
    // incoming one. (We'll additionally retain a random number of the
    // incoming voters below).
    auto nRemovedVoters = rng() % 3;

    // Voters, learners, and removed voters must not overlap. A "removed voter"
    // is one that we have in the outgoing config but not the incoming one.
    // | nVoters | nLearners | nRemovedVoters |
    std::vector<NodeId> ids;
    auto n = (nVoters + nLearners + nRemovedVoters) * 2;
    for (int i = 0; i < n * 2; i++) {
        ids.push_back(i + 1);
    }
    std::shuffle(ids.begin(), ids.end(), rng);
    for (auto i = 0; i < nVoters; ++i) {
        cs.voters.push_back(ids[i]);
    }
    ids.erase(ids.begin(), ids.begin() + nVoters);

    for (auto i = 0; i < nLearners; ++i) {
        cs.learners.push_back(ids[i]);
    }
    ids.erase(ids.begin(), ids.begin() + nLearners);

    // Roll the dice on how many of the incoming voters we decide were also
    // previously voters.
    //
    // NB: this code avoids creating non-nil empty slices (here and below).
    auto nOutgoingRetainedVoters = rng() % (nVoters + 1);
    for (auto i = 0; i < nOutgoingRetainedVoters; ++i) {
        cs.votersOutgoing.push_back(cs.voters[i]);
    }
    for (auto i = 0; i < nRemovedVoters; ++i) {
        cs.votersOutgoing.push_back(ids[i]);
    }

    // Only outgoing voters that are not also incoming voters can be in
    // LearnersNext (they represent demotions).
    if (nRemovedVoters > 0) {
        auto nLearnersNext = rng() % (nRemovedVoters + 1);
        for (auto i = 0; i < nLearnersNext; ++i) {
            cs.learnersNext.push_back(ids[i]);
        }
    }
    cs.autoLeave = ((!cs.votersOutgoing.empty()) && (rng() % 2 == 1));
    return cs;
}

bool equal(const raft::ConfState& l, const raft::ConfState& r)
{
    if (l.autoLeave != r.autoLeave) {
        return false;
    }

    auto eq = [](const auto& x, const auto& y) {
        if (x.size() != y.size()) {
            return false;
        }
        for (int i = 0; i < x.size(); ++i) {
            if (x[i] != y[i]) {
                return false;
            }
        }
        return true;
    };

    if (!eq(l.voters, r.voters)) {
        return false;
    }

    if (!eq(l.votersOutgoing, r.votersOutgoing)) {
        return false;
    }

    if (!eq(l.learners, r.learners)) {
        return false;
    }

    if (!eq(l.learnersNext, r.learnersNext)) {
        return false;
    }

    return true;
}

void check(raft::ConfState& cs)
{
    std::ranges::sort(cs.voters);
    std::ranges::sort(cs.votersOutgoing);
    std::ranges::sort(cs.learners);
    std::ranges::sort(cs.learnersNext);

    tracker::ProgressTracker tracker(20, 0);
    auto res = confchange::restore(cs, tracker, 20);

    tracker.reset(res->config_, res->progress_);

    // fmt::println("{}", res->config_.voters_);

    EXPECT_TRUE(res.has_value()) << res.error().what();

    auto cs2 = tracker.confState();
    auto ok = equal(cs, cs2);

    EXPECT_TRUE(ok);
}

TEST(ConfChange, restore)
{
    // Unit tests.
    raft::ConfState css[] = {
        ConfState{},
        ConfState{ .voters{ 1, 2, 3 } },
        ConfState{ .voters{ 1, 2, 3 }, .learners{ 4, 5, 6 } },
        ConfState{ .voters{ 1, 2, 3 }, .learners{ 5 }, .votersOutgoing{ 1, 2, 4, 6 }, .learnersNext{ 4 } },
    };

    for (auto i : std::views::iota(0uz, std::size(css))) {
        auto& cs = css[i];
        check(cs);
    }
}

TEST(ConfChange, restoreRandom)
{
    auto cs = generate();
    check(cs);
}

TEST(ConfChange, serialization)
{
    RandomGenerator<uint64_t> rng;
    ConfChange cc;
    cc.transition = ConfChangeTransition(rng() % 3);
    size_t size = rng() % 64;
    for (auto i : std::views::iota(0ul, size)) {
        cc.changes.push_back(ConfChangeItem{
          .type = ConfChangeType(rng() % 3),
          .nodeId = rng() % 1000,
        });
    }
    cc.context.resize(rng() % 1024);
    for (auto& i : cc.context) {
        cc.context = rng();
    }

    auto data = cc.serialize();
    ConfChange newCc;
    EXPECT_TRUE(newCc.parse(data));
    EXPECT_EQ(cc, newCc);
}