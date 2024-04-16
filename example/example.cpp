#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <error.h>
#include <node.h>
#include <raft.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>
#include <storage.h>

#include <raftpb/raft.pb.h>
#include <google/protobuf/compiler/plugin.h>

std::vector<std::string> parseCmd(const std::string& str)
{
    std::vector<std::string> cmd;

    size_t pos = -1;
    for (size_t i = 0; i < str.size(); ++i) {
        bool b = std::isblank(str[i]);
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

    uint64_t id_;
    raft::MemoryStorage storage_;
    raft::Node<raft::MemoryStorage> node_;
    std::thread thread_;

    std::mutex mutex_;
    std::unordered_map<std::string, std::string> kv_;

    std::string get(const std::string& key)
    {
        std::unique_lock<std::mutex> lk{ mutex_ };
        auto iter = kv_.find(key);
        if (iter != kv_.end()) {
            return iter->second;
        }
        return "";
    }

    void set(std::string key, std::string value)
    {
        std::unique_lock<std::mutex> lk{ mutex_ };
        kv_[key] = value;
    }

    void del(const std::string& key)
    {
        std::unique_lock<std::mutex> lk{ mutex_ };
        kv_.erase(key);
    }

    void proposal(std::string data)
    {
        std::unique_lock<std::mutex> lk{ mutex_ };
        proposals_.push_back(data);
    }

    std::vector<std::string> proposals()
    {
        std::unique_lock<std::mutex> lk{ mutex_ };
        return std::move(proposals_);
    }

    std::vector<std::string> proposals_;

    std::shared_ptr<spdlog::logger> logger_;
};

struct NodeStat
{
    raft::pb::HardState hardState_;
    raft::SoftState softState_;
};

class MailBox
{
public:
    void send(raft::pb::Message msg)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        msgs_[msg.to()].push_back(std::move(msg));
    }

    std::list<raft::pb::Message> recv(uint64_t id)
    {
        std::unique_lock<std::mutex> l{ mutex_ };
        return std::move(msgs_[id]);
    }

    NodeStat stat(uint64_t id)
    {
        std::unique_lock<std::mutex> l{ statsMutex_ };
        return stats_[id];
    }

    std::mutex mutex_;
    std::unordered_map<uint64_t, std::list<raft::pb::Message>> msgs_;
    std::mutex statsMutex_;
    std::map<uint64_t, NodeStat> stats_;
};

MailBox mailBox;

void run(NodeContext& nc)
{
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        auto proposals = nc.proposals();
        for (auto& p : proposals) {
            nc.proposal(p);
        }
        nc.node_.tick();

        auto msgs = mailBox.recv(nc.id_);
        for (auto& msg : msgs) {
            nc.logger_->info(msg.ShortDebugString());
            nc.node_.step(msg);
        }
        auto rd = nc.node_.ready();
        if (!rd) {
            continue;
        }

        auto& storage = nc.storage_;
        if (rd->hardState_) {
            {
                std::unique_lock<std::mutex> l{ mailBox.statsMutex_ };
                mailBox.stats_[nc.id_].hardState_ = *rd->hardState_;
            }

            storage.setHardState(*rd->hardState_);
        }
        if (rd->softState_) {
            {
                std::unique_lock<std::mutex> l{ mailBox.statsMutex_ };
                mailBox.stats_[nc.id_].softState_ = *rd->softState_;
            }
            // nc.logger_ << "soft state: " << rd->softState_->state_ << "," << rd->softState_->leaderId_;
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
            if (msg.type() != raft::pb::MsgHeartbeat && msg.type() != raft::pb::MsgHeartbeatResponse) {
                nc.logger_->info("msg type: {}", raft::pb::MessageType_Name(msg.type()));
            }
            mailBox.send(msg);
        }

        if (!rd->committedEntries_.empty()) {
            nc.logger_->info("committed size: {}", rd->committedEntries_.size());
        }

        for (auto& e : rd->committedEntries_) {
            if (e.type() == raft::pb::EntryNormal) {
                if (e.data().empty()) {
                    nc.logger_->info("empty entry: {}", e.ShortDebugString());
                    continue;
                }
                auto cmd = parseCmd(e.data());
                if (cmd[0] == "set" && cmd.size() == 3) {
                    nc.set(cmd[1], cmd[2]);
                    continue;
                } else if (cmd[0] == "del" && cmd.size() == 2) {
                    nc.del(cmd[1]);
                    continue;
                }
                nc.logger_->info("bad proposal: {}", e.data());
            }
        }
        nc.node_.advance();
    }
}

int main(int argc, char* argv[])
{
    // auto logger = spdlog::stdout_logger_mt("");
    // logger->error("wssfsderkirjyerpyepryeryeryery");

    std::vector<raft::Peer> peers;
    for (size_t i = 0; i < 5; i++) {
        peers.push_back(raft::Peer{ i + 1 });
    }

    std::vector<NodeContext*> nodes;
    for (size_t i = 0; i < 5; i++) {
        auto logger = spdlog::rotating_logger_st(fmt::format("{}", i + 1), fmt::format("example.log.{}", i + 1),
                                                 1000 * 1000 * 100, 3);
        raft::Config c{ *logger };
        c.heartbeatTick_ = 3;
        c.electionTick_ = 10;

        c.id_ = i + 1;
        auto nc = new NodeContext{ c };
        nc->logger_ = logger;
        nodes.push_back(nc);
        nodes.back()->thread_ = std::thread([nc, peers]() {
            try {
                nc->node_.bootstrap(peers);
                run(*nc);
            } catch (raft::Error err) {
                nc->logger_->info("err: {}", err.what());
            }
        });
    }

    while (true) {
        std::string line;
        std::getline(std::cin, line);
        auto cmd = parseCmd(line);
        if (cmd.size() < 2) {
            continue;
        }

        auto idx = std::atoi(cmd[0].c_str()) - 1;
        if (idx < 0 || idx >= nodes.size()) {
            std::cout << "bad idx" << std::endl;
            continue;
        }
        auto n = nodes[idx];

        auto& op = cmd[1];

        if (op == "get") {
            if (cmd.size() != 3) {
                continue;
            }
            std::cout << ">> " << n->get(cmd[2]) << std::endl;
        } else if (op == "set" || op == "del") {
            raft::pb::Message msg;
            msg.set_type(raft::pb::MsgPropose);
            msg.set_from(n->id_);
            msg.set_to(n->id_);
            std::string s;
            for (size_t i = 1; i < cmd.size(); i++) {
                s += cmd[i] + " ";
            }
            msg.add_entries()->set_data(s);
            mailBox.send(msg);
        }

        if (op == "stat") {
            auto stat = mailBox.stat(idx + 1);
            //std::cout << "hardState: " << stat.hardState_.ShortDebugString() << ", lead: " << stat.softState_.leaderId_
           //           << ", state: " << to_string(stat.softState_.state_) << std::endl;
        }

        // if (line == "s") {
        //     mailBox.printStat();
        //     continue;
        // }
        // for (auto nc : nodes) {
        //     raft::pb::Message msg;
        //     msg.set_type(raft::pb::MsgPropose);
        //     msg.set_from(nc->id_);
        //     msg.set_to(nc->id_);
        //     msg.add_entries()->set_data(line);
        //     mailBox.send(msg);
        // }
    }

    for (auto& i : nodes) {
        i->thread_.join();
    }

    return 0;
}