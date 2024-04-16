#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <random>
#include <vector>

#include <confchange/confchange.h>
#include <gtest/gtest.h>
#include <log/unstable.h>
#include <utils.h>

#include <raftpb/raft.pb.h>

std::mt19937 rng(std::random_device{}());

using namespace raft;

raft::pb::Entry buildEntry(Index i, Term t)
{
    raft::pb::Entry entry;
    entry.set_index(i);
    entry.set_term(t);
    return entry;
}

std::shared_ptr<raft::pb::Snapshot> buildSnapshot(Index i, Term t)
{
    auto snap = std::make_shared<raft::pb::Snapshot>();
    snap->mutable_metadata()->set_index(i);
    snap->mutable_metadata()->set_term(t);
    return snap;
}

TEST(Unstable, FirstIndex)
{
    auto snap = buildSnapshot(4, 1);

    struct
    {
        std::vector<raft::pb::Entry> entries;
        uint64_t offset;
        std::shared_ptr<raft::pb::Snapshot> snap;
        std::optional<Index> index;
    } tests[] = {
        // no snapshot
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          {},
        },
        {
          {},
          0,
          nullptr,
          {},
        },
        // has snapshot
        {
          { buildEntry(5, 1) },
          5,
          snap,
          5,
        },
        {
          {},
          5,
          snap,
          5,
        },
    };

    for (auto& tt : tests) {
        raft::Unstable u;
        u.init(tt.offset, tt.entries);
        u.setSnapshot(tt.snap);
        auto index = u.firstIndex();
        EXPECT_EQ(tt.index, index);
    }
}

TEST(Unstable, LastIndex)
{
    auto snap = buildSnapshot(4, 1);
    struct
    {
        std::vector<raft::pb::Entry> entries;
        uint64_t offset;
        std::shared_ptr<raft::pb::Snapshot> snap;
        std::optional<Index> index;
    } tests[] = {
        // last in entries
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          5,
        },
        {
          { buildEntry(5, 1) },
          5,
          snap,
          5,
        },
        // last in snapshot
        {
          {},
          5,
          snap,
          4,
        },
        // empty unstable
        {
          {},
          0,
          nullptr,
          {},
        },
    };

    for (size_t i = 0; i < std::size(tests); ++i) {
        auto& tt = tests[i];
        raft::Unstable u;
        u.init(tt.offset, tt.entries);
        u.setSnapshot(tt.snap);
        auto index = u.lastIndex();
        EXPECT_EQ(tt.index, index) << "test case : " << i;
    }
}

TEST(Unstable, Term)
{
    auto snap = buildSnapshot(4, 1);
    struct
    {
        std::vector<raft::pb::Entry> entries;
        uint64_t offset;
        std::shared_ptr<raft::pb::Snapshot> snap;
        Index index;
        std::optional<Term> term;
    } tests[] = {
        // term from entries
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          5,
          1,
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          6,
          {},
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          4,
          {},
        },
        {
          { buildEntry(5, 1) },
          5,
          snap,
          5,
          1,
        },
        {
          { buildEntry(5, 1) },
          5,
          snap,
          6,
          {},
        },
        // term from snapshot
        {
          { buildEntry(5, 1) },
          5,
          snap,
          4,
          1,
        },
        {
          { buildEntry(5, 1) },
          5,
          snap,
          3,
          {},
        },
        {
          {},
          5,
          snap,
          5,
          {},
        },
        {
          {},
          5,
          snap,
          4,
          1,
        },
        {
          {},
          0,
          nullptr,
          5,
          {},
        },
    };

    for (size_t i = 0; i < std::size(tests); ++i) {
        auto& tt = tests[i];
        raft::Unstable u;
        u.init(tt.offset, tt.entries);
        u.setSnapshot(tt.snap);
        auto term = u.term(tt.index);
        EXPECT_EQ(tt.term, term) << "test case: " << i << ", term: " << (term ? *term : 0)
                                 << ", expect: " << (tt.term ? *tt.term : 0);
    }
}

TEST(Unstable, Restore)
{
    raft::Unstable u;
    u.init(5, { buildEntry(5, 1) });
    u.setSnapshot(buildSnapshot(4, 1));
    // snapshotInProgress: true,
    auto s = buildSnapshot(6, 2);
    u.restore(s);

    EXPECT_EQ(s->metadata().index() + 1, u.offset());
}

TEST(Unstable, stableEntries)
{
    auto snap = buildSnapshot(4, 1);
    struct
    {
        std::vector<raft::pb::Entry> entries;
        Index offset;
        std::shared_ptr<raft::pb::Snapshot> snap;
        Index index;
        Term term;
        Index woffset;
        int wlen;
    } tests[] = {
        {
          {},
          0,
          nullptr,
          5,
          1,
          0,
          0,
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          5,
          1, // stable to the first entry
          6,
          0,
        },
        {
          { buildEntry(5, 1), buildEntry(6, 1) },
          5,
          nullptr,
          5,
          1, // stable to the first entry
          6,
          1,
        },
        {
          { buildEntry(5, 1), buildEntry(6, 1) },
          5,
          nullptr,
          5,
          1, // stable to the first entry and in-progress ahead
          6,
          1,
        },
        {
          { buildEntry(6, 2) },
          6,
          nullptr,
          6,
          1, // stable to the first entry and term mismatch
          6,
          1,
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          4,
          1, // stable to old entry
          5,
          1,
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          4,
          2, // stable to old entry
          5,
          1,
        },
        // with snapshot
        {
          { buildEntry(5, 1) },
          5,
          snap,
          5,
          1, // stable to the first entry
          6,
          0,
        },
        {
          { buildEntry(5, 1), buildEntry(6, 1) },
          5,
          snap,
          5,
          1, // stable to the first entry
          6,
          1,
        },
        {
          { buildEntry(5, 1), buildEntry(6, 1) },
          5,
          snap,
          5,
          1, // stable to the first entry and in-progress ahead
          6,
          1,
        },
        {
          { buildEntry(6, 2) },
          6,
          buildSnapshot(5, 1),
          6,
          1, // stable to the first entry and term mismatch
          6,
          1,
        },
        {
          { buildEntry(5, 1) },
          5,
          snap,
          4,
          1, // stable to snapshot
          5,
          1,
        },
        {
          { buildEntry(5, 2) },
          5,
          buildSnapshot(4, 2),
          4,
          1, // stable to old entry
          5,
          1,
        },
    };

    for (size_t i = 0; i < std::size(tests); ++i) {
        auto& tt = tests[i];
        raft::Unstable u;
        u.init(tt.offset, tt.entries);
        u.setSnapshot(tt.snap);
        u.stableEntries(tt.index, tt.term);
        EXPECT_EQ(tt.woffset, u.offset()) << "test case: " << i;
        EXPECT_EQ(tt.wlen, u.entries().size()) << "test case: " << i;
    }
}

TEST(Unstable, AppendEntries)
{
    auto snap = buildSnapshot(4, 1);
    struct
    {
        std::vector<raft::pb::Entry> entries;
        Index offset;
        std::shared_ptr<raft::pb::Snapshot> snap;
        std::vector<raft::pb::Entry> toappend;

        Index woffset;
        Index woffsetInProgress;
        std::vector<raft::pb::Entry> wentries;
    } tests[] = {
        // append to the end
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          { buildEntry(6, 1), buildEntry(7, 1) },
          5,
          5,
          { buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1) },
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          { buildEntry(6, 1), buildEntry(7, 1) },
          5,
          6,
          { buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1) },
        },
        // replace the unstable entries
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          { buildEntry(5, 2), buildEntry(6, 2) },
          5,
          5,
          { buildEntry(5, 2), buildEntry(6, 2) },
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          { buildEntry(4, 2), buildEntry(5, 2), buildEntry(6, 2) },
          4,
          4,
          { buildEntry(4, 2), buildEntry(5, 2), buildEntry(6, 2) },
        },
        {
          { buildEntry(5, 1) },
          5,
          nullptr,
          { buildEntry(5, 2), buildEntry(6, 2) },
          5,
          5,
          { buildEntry(5, 2), buildEntry(6, 2) },
        },
        // truncate the existing entries and append
        {
          { buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1) },
          5,
          nullptr,
          { buildEntry(6, 2) },
          5,
          5,
          { buildEntry(5, 1), buildEntry(6, 2) },
        },
        {
          { buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1) },
          5,
          nullptr,
          { buildEntry(7, 2), buildEntry(8, 2) },
          5,
          5,
          { buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 2), buildEntry(8, 2) },
        },
        {
          { buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1) },
          5,
          nullptr,
          { buildEntry(6, 2) },
          5,
          6,
          { buildEntry(5, 1), buildEntry(6, 2) },
        },
        {
          { buildEntry(5, 1), buildEntry(6, 1), buildEntry(7, 1) },
          5,
          nullptr,
          { buildEntry(6, 2) },
          5,
          6,
          { buildEntry(5, 1), buildEntry(6, 2) },
        },
    };

    for (size_t i = 0; i < std::size(tests); ++i) {
        auto& tt = tests[i];
        raft::Unstable u;
        u.init(tt.offset, tt.entries);
        u.setSnapshot(tt.snap);
        google::protobuf::RepeatedPtrField<raft::pb::Entry> toappend;
        for (auto& i : tt.toappend) {
            toappend.Add(std::move(i));
        }
        u.appendEntries(toappend);
        EXPECT_EQ(tt.woffset, u.offset());
        EXPECT_EQ(u.entries().size(), tt.wentries.size());
        for (size_t i = 0; i < u.entries().size(); ++i) {
            EXPECT_EQ(u.entries()[i].index(), tt.wentries[i].index());
            EXPECT_EQ(u.entries()[i].term(), tt.wentries[i].term());
        }
    }
}