#include <algorithm>
#include <random>
#include <vector>

#include <confchange/confchange.h>
#include <gtest/gtest.h>
#include <tracker/tracker.h>

#include <raftpb/raft.pb.h>

std::mt19937 rng(std::random_device{}());

using namespace raft;

// Generate creates a random (valid) ConfState for use with quickcheck.
raft::pb::ConfState generate()
{
    raft::pb::ConfState cs;
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
    std::vector<uint64_t> ids;
    auto n = (nVoters + nLearners + nRemovedVoters) * 2;
    for (int i = 0; i < n * 2; i++) {
        ids.push_back(i + 1);
    }
    std::shuffle(ids.begin(), ids.end(), rng);
    for (auto i = 0; i < nVoters; ++i) {
        cs.mutable_voters()->Add(ids[i]);
    }
    ids.erase(ids.begin(), ids.begin() + nVoters);

    for (auto i = 0; i < nLearners; ++i) {
        cs.mutable_learners()->Add(ids[i]);
    }
    ids.erase(ids.begin(), ids.begin() + nLearners);

    // Roll the dice on how many of the incoming voters we decide were also
    // previously voters.
    //
    // NB: this code avoids creating non-nil empty slices (here and below).
    auto nOutgoingRetainedVoters = rng() % (nVoters + 1);
    for (auto i = 0; i < nOutgoingRetainedVoters; ++i) {
        cs.mutable_voters_outgoing()->Add(cs.voters(i));
    }
    for (auto i = 0; i < nRemovedVoters; ++i) {
        cs.mutable_voters_outgoing()->Add(ids[i]);
    }

    // Only outgoing voters that are not also incoming voters can be in
    // LearnersNext (they represent demotions).
    if (nRemovedVoters > 0) {
        auto nLearnersNext = rng() % (nRemovedVoters + 1);
        for (auto i = 0; i < nLearnersNext; ++i) {
            cs.mutable_learners_next()->Add(ids[i]);
        }
    }
    cs.set_auto_leave((!cs.voters_outgoing().empty()) && (rng() % 2 == 1));
    return cs;
}

bool equal(const raft::pb::ConfState& l, const raft::pb::ConfState& r)
{
    if (l.auto_leave() != r.auto_leave()) {
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

    if (!eq(l.voters(), r.voters())) {
        return false;
    }

    if (!eq(l.voters_outgoing(), r.voters_outgoing())) {
        return false;
    }

    if (!eq(l.learners(), r.learners())) {
        return false;
    }

    if (!eq(l.learners_next(), r.learners_next())) {
        return false;
    }

    return true;
}

raft::pb::ConfState genConfState(const std::vector<uint64_t>& voters, const std::vector<uint64_t>& votersOutgoing,
                                 const std::vector<uint64_t>& learners, const std::vector<uint64_t>& learnersNext)
{
    raft::pb::ConfState s;
    s.mutable_voters()->Assign(voters.begin(), voters.end());
    s.mutable_voters_outgoing()->Assign(votersOutgoing.begin(), votersOutgoing.end());
    s.mutable_learners()->Assign(learners.begin(), learners.end());
    s.mutable_learners_next()->Assign(learnersNext.begin(), learnersNext.end());
    return s;
}

void check(raft::pb::ConfState& cs)
{
    tracker::ProgressTracker tracker(20, 0);
    auto res = confchange::restore(cs, [&]() { return tracker.create(20, true); });

    tracker.update(res->config_, res->progress_);

    EXPECT_TRUE(res.has_value()) << res.error().what();

    std::sort(cs.mutable_voters()->begin(), cs.mutable_voters()->end());
    std::sort(cs.mutable_voters_outgoing()->begin(), cs.mutable_voters_outgoing()->end());
    std::sort(cs.mutable_learners()->begin(), cs.mutable_learners()->end());
    std::sort(cs.mutable_learners_next()->begin(), cs.mutable_learners_next()->end());

    auto cs2 = tracker.confState();
    auto ok = equal(cs, cs2);

    EXPECT_TRUE(ok) << "origin : " << cs.ShortDebugString() << "\nrestore: " << cs2.ShortDebugString();
}

TEST(Confchange, restore)
{
    // Unit tests.
    raft::pb::ConfState css[] = {
        genConfState({}, {}, {}, {}),
        genConfState({ 1, 2, 3 }, {}, {}, {}),
        genConfState({ 1, 2, 3 }, {}, { 4, 5, 6 }, {}),
        genConfState({ 1, 2, 3 }, { 1, 2, 4, 6 }, { 5 }, { 4 }),
    };

    for (auto& cs : css) {
        check(cs);
    }
}

TEST(Confchange, restoreRandom)
{
    auto cs = generate();
    check(cs);
}