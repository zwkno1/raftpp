
#pragma once

#include <raftpb/raft.pb.h>

namespace raft {

template <typename T>
concept Logger = requires(T t) {
    {
        t.debug()
    };
};

}