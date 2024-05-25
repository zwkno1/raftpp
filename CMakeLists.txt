cmake_minimum_required(VERSION 3.6)

project(raft)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 23)

option(WITH_TESTS "enable test" ON)
option(WITH_BENCHMARKS "enable benchmark" ON)

include_directories(${PROJECT_SOURCE_DIR}/include)

include(FetchContent)
set(FETCHCONTENT_QUIET OFF)

# spdlog
set(SPDLOG_USE_STD_FORMAT ON)
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG v1.14.1)
FetchContent_MakeAvailable(spdlog)

if(WITH_TESTS)
  # gtest
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0)
  # For Windows: Prevent overriding the parent project's compiler/linker
  # settings
  set(gtest_force_shared_crt
      ON
      CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  include(GoogleTest)
  enable_testing()

  add_executable(quorum_test test/quorum_test.cpp)
  target_link_libraries(quorum_test spdlog::spdlog GTest::gtest_main)
  gtest_discover_tests(quorum_test)

  add_executable(inflights_test test/inflights_test.cpp)
  target_link_libraries(inflights_test spdlog::spdlog GTest::gtest_main)
  gtest_discover_tests(inflights_test)

  add_executable(confchange_test test/confchange_test.cpp)
  target_link_libraries(confchange_test spdlog::spdlog GTest::gtest_main)
  gtest_discover_tests(confchange_test)

  add_executable(log_test test/log_test.cpp)
  target_link_libraries(log_test spdlog::spdlog GTest::gtest_main)
  gtest_discover_tests(log_test)

endif(WITH_TESTS)

if(WITH_BENCHMARKS)
  # benchmark
  set(BENCHMARK_ENABLE_TESTING
      OFF
      CACHE INTERNAL "")
  # Do not build and run googlebenchmark tests
  FetchContent_Declare(
    googlebenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.8.3)
  FetchContent_MakeAvailable(googlebenchmark)

  add_executable(bench_quorum benchmark/bench_quorum.cpp)
  target_link_libraries(bench_quorum spdlog::spdlog benchmark::benchmark)
endif(WITH_BENCHMARKS)

add_executable(raft_example example/example.cpp)
target_link_libraries(raft_example spdlog::spdlog)
