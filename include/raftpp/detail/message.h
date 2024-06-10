#pragma once

#include <algorithm>
#include <bit>
#include <concepts>
#include <format>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>

namespace raft {

using Index = uint64_t;
using Term = uint64_t;
using NodeId = uint64_t;

constexpr Index IndexMax = std::numeric_limits<Index>::max();

// InvalidId is a placeholder node ID used when there is no leader.
constexpr NodeId InvalidId = 0;

struct IndexTerm
{
    Index index = 0;
    Term term = 0;
};

enum CampaignType : uint32_t
{
    // campaignPreElection represents the first phase of a normal election when
    // Config.PreVote is true.
    CampaignPreElection = 0,
    // campaignElection represents a normal (time-based) election (the second phase
    // of the election when Config.PreVote is true).
    CampaignElection,
    // campaignTransfer represents the type of leader transfer
    CampaignTransfer,
    CampaignTypeInvalid,
};

enum EntryType : uint32_t
{
    EntryNormal = 0,
    EntryConfChange = 1,
    EntryEmpty = 2,
};

struct Entry
{
    EntryType type = EntryNormal;
    Index index = 0;
    Term term = 0;
    std::string data;

    inline size_t payload() const { return 32 + data.size(); }
};

using EntryList = std::vector<Entry>;
using EntrySlice = std::span<Entry>;
using ConstEntrySlice = std::span<const Entry>;

struct ConfState
{
    // The voters in the incoming config. (If the configuration is not joint,
    // then the outgoing config is empty).
    std::unordered_set<NodeId> voters;
    // The learners in the incoming config.
    std::unordered_set<NodeId> learners;
    // The voters in the outgoing config.
    std::unordered_set<NodeId> votersOutgoing;
    // The nodes that will become learners when the outgoing config is removed.
    // These nodes are necessarily currently in nodes_joint (or they would have
    // been added to the incoming config right away).
    std::unordered_set<NodeId> learnersNext;
    // If set, the config is joint and Raft will automatically transition into
    // the final config (i.e. remove the outgoing config) when this is safe.
    bool autoLeave = false;

    bool operator==(const ConfState&) const = default;
};

struct HardState
{
    Term term = 0;
    Index commit = 0;
    NodeId vote = 0;

    bool empty() const { return term == 0 && commit == 0 && vote == 0; }

    inline bool operator==(const HardState& other) const
    {
        return (term == other.term) && (vote == other.vote) && (commit == other.commit);
    }
};

struct State
{
    HardState hard;
    ConfState conf;
};

struct SnapshotMeta
{
    ConfState confState;
    Index index = 0;
    Term term = 0;
};

struct Snapshot
{
    SnapshotMeta meta;
    std::string data;

    bool empty() const { return meta.index == 0; }
};

// ConfChangeTransition specifies the behavior of a configuration change with
// respect to joint consensus.
enum ConfChangeTransition : uint32_t
{
    // Automatically use the simple protocol if possible, otherwise fall back
    // to ConfChangeJointImplicit. Most applications will want to use this.
    Auto = 0,
    // Use joint consensus unconditionally, and transition out of them
    // automatically (by proposing a zero configuration change).
    //
    // This option is suitable for applications that want to minimize the time
    // spent in the joint configuration and do not store the joint configuration
    // in the state machine (outside of InitialState).
    Implicit = 1,
    // Use joint consensus and remain in the joint configuration until the
    // application proposes a no-op configuration change. This is suitable for
    // applications that want to explicitly control the transitions, for example
    // to use a custom payload (via the Context field).
    Explicit = 2,
};

enum ConfChangeType : uint32_t
{
    AddNode = 0,
    RemoveNode = 1,
    AddLearnerNode = 2,
};

// ConfChangeSingle is an individual configuration change operation. Multiple
// such operations can be carried out atomically via a ConfChangeV2.
struct ConfChangeItem
{
    ConfChangeType type;
    NodeId nodeId;
    auto operator<=>(const ConfChangeItem&) const = default;
};

// ConfChange structs initiate configuration changes. They support both the
// simple "one at a time" membership change protocol and full Joint Consensus
// allowing for arbitrary changes in membership.
//
// The supplied context is treated as an opaque payload and can be used to
// attach an action on the state machine to the application of the config change
// proposal. Note that contrary to Joint Consensus as outlined in the Raft
// paper[1], configuration changes become active when they are *applied* to the
// state machine (not when they are appended to the log).
//
// The simple protocol can be used whenever only a single change is made.
//
// Non-simple changes require the use of Joint Consensus, for which two
// configuration changes are run. The first configuration change specifies the
// desired changes and transitions the Raft group into the joint configuration,
// in which quorum requires a majority of both the pre-changes and post-changes
// configuration. Joint Consensus avoids entering fragile intermediate
// configurations that could compromise survivability. For example, without the
// use of Joint Consensus and running across three availability zones with a
// replication factor of three, it is not possible to replace a voter without
// entering an intermediate configuration that does not survive the outage of
// one availability zone.
//
// The provided ConfChangeTransition specifies how (and whether) Joint Consensus
// is used, and assigns the task of leaving the joint configuration either to
// Raft or the application. Leaving the joint configuration is accomplished by
// proposing a ConfChangeV2 with only and optionally the Context field
// populated.
//
// For details on Raft membership changes, see:
//
// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
struct ConfChange
{
    ConfChangeTransition transition;
    std::vector<ConfChangeItem> changes;
    std::string context;

    auto operator<=>(const ConfChange&) const = default;

    inline std::string serialize() const
    {
        std::string buf;
        buf.reserve(2 + sizeof(transition) + sizeof(uint64_t) + changes.size() * sizeof(ConfChangeItem) +
                    sizeof(uint64_t) + context.size());
        buf.append("v1", 2);

        auto write = [&](auto v) {
            if (std::endian::native == std::endian::big) {
                v = std::byteswap(v);
            }
            buf.append((const char*)&v, sizeof(v));
        };

        write(uint32_t(transition));
        write(uint64_t{ changes.size() });
        for (auto& c : changes) {
            write(uint32_t(c.type));
            write(c.nodeId);
        }
        write(uint64_t{ context.size() });
        buf.append(context);

        return buf;
    }

    inline bool parse(const std::string& data)
    {
        if (data.size() <= 2) {
            return false;
        }
        std::string_view ver{ data.data(), 2 };
        if (ver != "v1") {
            return false;
        }
        size_t pos = 2;

        auto parse = [&](auto& v) {
            if (sizeof(v) + pos > data.size()) {
                return false;
            }
            v = *reinterpret_cast<const std::remove_reference_t<decltype(v)>*>(data.data() + pos);
            if (std::endian::native == std::endian::big) {
                v = std::byteswap(v);
            }
            pos += sizeof(v);
            return true;
        };
        uint64_t size;
        uint32_t type;
        if (!parse(type)) {
            return false;
        }
        transition = ConfChangeTransition{ type };
        if (!parse(size)) {
            return false;
        }
        for (auto i : std::views::iota(0uz, size)) {
            ConfChangeItem item;
            if (!parse(type)) {
                return false;
            }
            item.type = ConfChangeType{ type };
            if (!parse(item.nodeId)) {
                return false;
            }
            changes.push_back(item);
        }

        if (!parse(size)) {
            return false;
        }
        if (size + pos != data.size()) {
            return false;
        }
        context = data.substr(pos);
        return true;
    }
};

struct ProposalRequst
{
    NodeId from = 0;
    NodeId to = 0;
    EntryList entries;
};

struct AppendEntriesRequest
{
    NodeId from = 0;
    NodeId to = 0;
    // leader’s term
    Term term = 0;

    // index and term of log entry immediately preceding new ones
    IndexTerm prevLog;

    // log entries to store (empty for heartbeat; may send more than one for efficiency)
    EntryList entries;

    // leader’s commitIndex
    Index commit = 0;
};

struct AppendEntriesResponse
{
    NodeId from = 0;
    NodeId to = 0;
    Index index = 0;

    // currentTerm, for leader to update itself
    Term term = 0;

    // true if follower contained entry matching prevLog(Index and Term)
    bool reject = true;

    IndexTerm rejectHint;
};

struct HeartbeatRequest
{
    NodeId from = 0;
    NodeId to = 0;

    Term term = 0;
    Index commit = 0;
    std::string context;
};

struct HeartbeatResponse
{
    NodeId from = 0;
    NodeId to = 0;

    Term term = 0;

    std::string context;
};

struct VoteRequest
{
    NodeId from = 0;
    NodeId to = 0;
    // is pre vote
    bool pre = false;

    // candidate’s term
    Term term = 0;

    // index and term of candidate’s last log entry (§5.4)
    IndexTerm lastLog;

    CampaignType type;
};

struct VoteResponse
{
    NodeId from = 0;
    NodeId to = 0;
    // is pre vote
    bool pre = false;
    Term term = 0;
    bool reject = true;
};

struct InstallSnapshotRequest
{
    NodeId from = 0;
    NodeId to = 0;
    Term term;
    Snapshot snapshot;
};

struct InstallSnapshotResponse
{
    NodeId from = 0;
    NodeId to = 0;
    Term term;
    Index index;
};

struct ReadIndexRequest
{
    NodeId from = 0;
    NodeId to = 0;
    std::string context;
};

struct ReadIndexResponse
{
    NodeId from = 0;
    NodeId to = 0;
    Index index = 0;

    Term term = 0;

    std::string context;
};

struct TransferLeaderRequest
{
    NodeId from = 0;
    NodeId to = 0;
    Term term = 0;
};

struct TimeoutNowRequest
{
    NodeId from = 0;
    NodeId to = 0;
    Term term = 0;
};

template <typename T>
size_t payloadSize(const T& x)
{
    if constexpr (std::is_same_v<Entry, T>) {
        return x.payload();
    } else if constexpr (std::ranges::range<T>) {
        size_t size = 0;
        std::ranges::for_each(x, [&size](auto& e) { size += e.payload(); });
        return size;
    }
}

// clang-format off

using MessageHolder = std::variant<VoteRequest,
                                   VoteResponse, 
                                   AppendEntriesRequest, 
                                   AppendEntriesResponse,
                                   HeartbeatRequest,
                                   HeartbeatResponse,
                                   ProposalRequst, 
                                   InstallSnapshotRequest, 
                                   InstallSnapshotResponse,
                                   TransferLeaderRequest,
                                   ReadIndexRequest,
                                   ReadIndexResponse,
                                   TimeoutNowRequest
                                   >;
// clang-format on

template <typename T, typename... Args>
constexpr bool one_of =
  std::disjunction<std::is_same<std::remove_reference_t<T>, Args>...>::value; // one_of_t<std::remove_reference<T>,
                                                                              // Args...>::value;

template <typename T, typename U>
struct one_of_variant;

template <typename T, typename... Args>
struct one_of_variant<T, std::variant<Args...>> : public std::disjunction<std::is_same<T, Args>...>
{
};

template <typename T>
concept Message = one_of_variant<std::remove_reference_t<T>, MessageHolder>::value;

} // namespace raft

template <typename CharT>
struct std::formatter<raft::CampaignType, CharT> : std::formatter<string_view, CharT>
{
    template <class FormatContext>
    auto format(raft::CampaignType t, FormatContext& ctx) const
    {
        static const std::array<std::string_view, 4> names = {
            "CampaignPreElection",
            "CampaignElection",
            "CampaignTransfer",
            "CampaignTypeInvalid",
        };

        if (t > raft::CampaignTypeInvalid) {
            t = raft::CampaignTypeInvalid;
        }

        return std::format_to(ctx.out(), "{}", names[t]);
    }
};
