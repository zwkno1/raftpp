
include_directories(${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS raft.proto)

add_library(raftpb ${PROTO_HDRS} ${PROTO_SRCS})

target_link_libraries(raftpb protobuf::libprotobuf)