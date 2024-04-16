#include <chrono>
#include <iostream>

#include <benchmark/benchmark.h>
#include <quorum/majority.h>
#include <quorum/quorum.h>

using namespace raft;
using namespace raft::quorum;

static void BenchmarkMajorityConfigCommittedIndex(benchmark::State& state)
{
    MajorityConfig c;
    MapAckIndexer l;
    for (uint64_t i = 0; i < state.range(0); i++) {
        c.add(i + 1);
        l.add(i + 1, std::rand());
    }

    for (auto _ : state) {
        Index i = c.committedIndex(l);
        benchmark::DoNotOptimize(i);
    }
}

BENCHMARK(BenchmarkMajorityConfigCommittedIndex)->DenseRange(1, 11, 2);

BENCHMARK_MAIN();