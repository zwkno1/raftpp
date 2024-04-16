cmake_minimum_required(VERSION 3.6)

project(raft)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 23)

option(WITH_TESTS "enable test" ON)


# FetchContent_Declare(protobuf URL
# https://github.com/protocolbuffers/protobuf/archive/refs/tags/v27.0-rc1.tar.gz
# SOURCE_SUBDIR  cmake FIND_PACKAGE_ARGS NAMES protobuf )

# set(Protobuf_USE_STATIC_LIBS OFF)
find_package(protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(./ ${CMAKE_CURRENT_BINARY_DIR})


include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# benchmark
set(BENCHMARK_ENABLE_TESTING
    OFF
    CACHE INTERNAL "")
# Do not build and run googlebenchmark tests
FetchContent_Declare(
  googlebenchmark
  URL https://github.com/google/benchmark/archive/refs/tags/v1.8.3.tar.gz)
FetchContent_MakeAvailable(googlebenchmark)

# gtest
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt
    ON
    CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# spdlog
FetchContent_Declare(
  spdlog URL https://github.com/gabime/spdlog/archive/refs/tags/v1.14.1.tar.gz)
FetchContent_MakeAvailable(spdlog)

add_subdirectory(raftpb)
if(WITH_TESTS)
  include(GoogleTest)
  enable_testing()

  add_executable(quorum_test quorum/quorum_test.cpp)
  target_link_libraries(quorum_test spdlog::spdlog raftpb GTest::gtest_main)
  gtest_discover_tests(quorum_test)

  add_executable(inflights_test tracker/inflights_test.cpp)
  target_link_libraries(inflights_test spdlog::spdlog raftpb GTest::gtest_main)
  gtest_discover_tests(inflights_test)

  add_executable(confchange_test confchange/confchange_test.cpp)
  target_link_libraries(confchange_test raftpb protobuf::libprotobuf
                        spdlog::spdlog GTest::gtest_main)
  gtest_discover_tests(confchange_test)

  add_executable(log_test log/log_test.cpp)
  target_link_libraries(log_test raftpb protobuf::libprotobuf spdlog::spdlog
                        GTest::gtest_main)
  gtest_discover_tests(log_test)

endif()

add_executable(bench_quorum quorum/bench_quorum.cpp)
target_link_libraries(bench_quorum raftpb spdlog::spdlog benchmark::benchmark)

add_executable(raft_example example/example.cpp)
target_link_libraries(raft_example raftpb spdlog::spdlog)

#add_library(plugin SHARED example/plugin.h)
#set_target_properties(plugin PROPERTIES LINKER_LANGUAGE CXX)
#target_link_libraries(plugin protobuf::libprotobuf)
#
#find_package(absl REQUIRED)
#
#include_directories(example)
#add_executable(
#  protoc-gen-demo
#  #example/protoc-gen-demo.cpp
#  example/cpp_simple/main.cc
#  example/cpp_simple/enum.cc
#  example/cpp_simple/extension.cc
#  example/cpp_simple/message.cc
#  example/cpp_simple/field.cc
#  example/cpp_simple/namespace_printer.cc
#  example/cpp_simple/file.cc
#  example/cpp_simple/padding_optimizer.cc
#  example/cpp_simple/generator.cc
#  example/cpp_simple/parse_function_generator.cc
#  example/cpp_simple/helpers.cc
#  example/cpp_simple/service.cc
#  example/cpp_simple/ifndef_guard.cc
#  example/cpp_simple/tracker.cc
#  example/cpp_simple/field_generators/cord_field.cc
#  example/cpp_simple/field_generators/primitive_field.cc
#  example/cpp_simple/field_generators/enum_field.cc
#  example/cpp_simple/field_generators/string_field.cc
#  example/cpp_simple/field_generators/map_field.cc
#  example/cpp_simple/field_generators/string_view_field.cc
#  example/cpp_simple/field_generators/message_field.cc)
#
#target_link_libraries(
#  protoc-gen-demo
#  protobuf::libprotobuf
#  protobuf::libprotoc
#  absl::algorithm
#  absl::base
#  absl::debugging
#  absl::flat_hash_map
#  absl::flags
#  absl::memory
#  absl::meta
#  absl::numeric
#  absl::random_random
#  absl::strings
#  absl::synchronization
#  absl::time
#  absl::utility)
#