

#include <iostream>
#include <list>
#include <print>

#include <raftpp/raftpp.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

#include "memorystorage.h"

using namespace std;

vector<string> parseCmd(const string& str)
{
    vector<string> cmd;

    size_t pos = -1;
    for (size_t i = 0; i < str.size(); ++i) {
        bool b = isblank(str[i]);
        if (pos == -1) {
            if (!b) {
                pos = i;
            }
        } else {
            if (b) {
                cmd.push_back(str.substr(pos, i - pos));
                pos = -1;
            }
        }
    }
    if (pos != -1) {
        cmd.push_back(str.substr(pos));
    }
    return cmd;
}

struct NodeContext
{
    NodeContext(raft::Config& c)
      : id_(c.id_)
      , node_(c, storage_)
    {
    }

    raft::NodeId id_;
    MemoryStorage storage_;
    raft::Node<MemoryStorage> node_;
    thread thread_;

    mutex mutex_;
    unordered_map<string, string> kv_;

    string get(const string& key)
    {
        unique_lock<mutex> lk{ mutex_ };
        auto iter = kv_.find(key);
        if (iter != kv_.end()) {
            return iter->second;
        }
        return "";
    }

    void set(string key, string value)
    {
        unique_lock<mutex> lk{ mutex_ };
        kv_[key] = value;
    }

    void del(const string& key)
    {
        unique_lock<mutex> lk{ mutex_ };
        kv_.erase(key);
    }

    void proposal(string data)
    {
        unique_lock<mutex> lk{ mutex_ };
        proposals_.push_back(data);
    }

    vector<string> proposals()
    {
        unique_lock<mutex> lk{ mutex_ };
        return std::move(proposals_);
    }

    vector<string> proposals_;

    shared_ptr<spdlog::logger> logger_;
};

struct NodeStat
{
    raft::HardState hardState_;
    raft::SoftState softState_;
};

class MailBox
{
public:
    template <raft::Message Msg>
    void send(Msg msg)
    {
        unique_lock<mutex> l{ mutex_ };
        msgs_[msg.to].push_back(std::move(msg));
    }

    list<raft::MessageHolder> recv(raft::NodeId id)
    {
        unique_lock<mutex> l{ mutex_ };
        return std::move(msgs_[id]);
    }

    NodeStat stat(raft::NodeId id)
    {
        unique_lock<mutex> l{ statsMutex_ };
        return stats_[id];
    }

    mutex mutex_;
    unordered_map<raft::NodeId, list<raft::MessageHolder>> msgs_;
    mutex statsMutex_;
    map<raft::NodeId, NodeStat> stats_;
};

MailBox mailBox;

void run(NodeContext& nc)
{
    while (true) {
        this_thread::sleep_for(chrono::milliseconds(100));
        // this_thread::sleep_for(chrono::seconds(1));
        auto proposals = nc.proposals();
        // nc.logger_->debug("get proposal, size: {}", proposals.size());
        for (auto& p : proposals) {
            // nc.logger_->debug("proposal: {}", p);
            nc.node_.propose(p);
        }
        nc.node_.tick();

        auto msgs = mailBox.recv(nc.id_);
        for (auto& msg : msgs) {
            std::visit([&](auto&& msg) { nc.node_.step(msg); }, msg);
        }

        auto rd = nc.node_.ready();
        if (!rd) {
            continue;
        }

        auto& storage = nc.storage_;
        if (rd->hardState_) {
            {
                unique_lock<mutex> l{ mailBox.statsMutex_ };
                mailBox.stats_[nc.id_].hardState_ = *rd->hardState_;
            }

            storage.setHardState(*rd->hardState_);
        }
        if (rd->softState_) {
            {
                unique_lock<mutex> l{ mailBox.statsMutex_ };
                mailBox.stats_[nc.id_].softState_ = *rd->softState_;
            }
        }

        if (rd->snapshot_) {
            nc.logger_->info("snapshot");
            storage.applySnapshot(*rd->snapshot_);
        }

        if (!rd->entries_.empty()) {
            storage.append(rd->entries_);
        }

        if (!rd->readStates_.empty()) {
            nc.logger_->info("readstates size: {}", rd->readStates_.size());
        }

        for (auto& msg : rd->msgs_) {
            std::visit([&](auto&& m) { mailBox.send(m); }, msg);
            // if (msg.type != raft::MsgHeartbeat && msg.type != raft::MsgHeartbeatResponse) {
            //  nc.logger_->info("msg type: {}", raft::MessageType_Name(msg.type));
            //}
        }

        if (!rd->committedEntries_.empty()) {
            nc.logger_->info("committed size: {}", rd->committedEntries_.size());
        }

        for (auto& e : rd->committedEntries_) {
            if (e.type == raft::EntryNormal) {
                nc.logger_->info("proposal: {}", e.data);
                if (e.data.empty()) {
                    nc.logger_->info("empty entry");
                    continue;
                }
                auto cmd = parseCmd(e.data);
                if (cmd[0] == "set" && cmd.size() == 3) {
                    nc.set(cmd[1], cmd[2]);
                    continue;
                } else if (cmd[0] == "del" && cmd.size() == 2) {
                    nc.del(cmd[1]);
                    continue;
                }
                nc.logger_->info("bad proposal: {}", e.data);
            }
        }
        nc.node_.advance();
    }
}

int main(int argc, char* argv[])
{
    // nodes number
    constexpr int n = 3;

    vector<raft::Peer> peers;
    for (size_t i : std::views::iota(0, n)) {
        peers.push_back(raft::Peer{ i + 1 });
    }

    vector<NodeContext*> nodes;
    for (size_t i : std::views::iota(0, n)) {
        auto logger = spdlog::basic_logger_mt(std::format("{}", i + 1), std::format("example.{}.log", i + 1));
        // logger->flush_on(spdlog::level::info);
        // auto logger = spdlog::stdout_logger_mt(std::format("{}", i + 1));
        // if (i == 0)
        logger->set_level(spdlog::level::trace);
        raft::Config c{ *logger };
        c.heartbeatTick_ = 3;
        c.electionTick_ = 10;

        c.id_ = i + 1;
        auto nc = new NodeContext{ c };
        nc->logger_ = logger;
        nodes.push_back(nc);
        nodes.back()->thread_ = thread([nc, peers]() {
            try {
                nc->node_.bootstrap(peers);
                run(*nc);
            } catch (raft::Error err) {
                nc->logger_->info("err: {}", err.what());
            }
        });
    }

    auto genStr = [](size_t sz) {
        std::string s;
        for (; sz != 0; --sz) {
            s.push_back('a' + rand() % 26);
        }
        return s;
    };

    // std::thread([&] {
    //     for (;;) {
    //         this_thread::sleep_for(chrono::milliseconds(10));
    //         auto idx = rand() % n;
    //         auto cmd = "set " + genStr(rand() % 2 + 1) + " " + genStr(rand() % 5 + 5);
    //         // raft::ProposalRequst msg;
    //         // msg.entries.push_back(raft::Entry{ .data = std::move(cmd) });
    //         nodes[idx]->proposal(cmd);
    //         // if(rand()%2 == 0) {
    //         //     cmd = "del";
    //         // }
    //     }
    // }).detach();

    while (true) {
        string line;
        getline(std::cin, line);
        auto cmd = parseCmd(line);
        if (cmd.empty()) {
            continue;
        }

        auto op = cmd[0];
        if (op == "s") {
            auto printStat = [&](int id) {
                auto stat = mailBox.stat(id);
                std::println("[{}] term: {}, commit: {}, vote: {}, lead: {}, state: {}", id, stat.hardState_.term,
                             stat.hardState_.commit, stat.hardState_.vote, stat.softState_.lead_,
                             stat.softState_.state_);
            };

            for (auto i : std::views::iota(1ul, nodes.size() + 1)) {
                printStat(i);
            }
            continue;
        }

        if (cmd.size() < 2) {
            continue;
        }

        op = cmd[1];

        auto id = atoi(cmd[0].c_str());
        if (id <= 0 || id > nodes.size()) {
            std::println("bad idx");
            continue;
        }
        auto n = nodes[id - 1];

        if (op == "get") {
            if (cmd.size() != 3) {
                continue;
            }
            std::println(">> {}", n->get(cmd[2]));
        } else if (op == "set" || op == "del") {
            // raft::ProposalRequst msg;
            string s;
            for (size_t i = 1; i < cmd.size(); i++) {
                s += cmd[i] + " ";
            }
            // msg.entries.push_back(raft::Entry{ .data = s });
            n->proposal(s);
        }
    }

    for (auto& i : nodes) {
        i->thread_.join();
    }

    return 0;
}