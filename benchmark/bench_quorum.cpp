#include <chrono>
#include <random>

#include <benchmark/benchmark.h>
#include <raftpp/raftpp.h>

using namespace raft;
using namespace raft::quorum;

RandomGenerator<uint64_t> rng{ 0, 1ull << 63 };

void BenchmarkMajorityConfigCommittedIndex(benchmark::State& state)
{
    MajorityConfig c;
    MapAckIndexer l;
    for (auto i = 0; i < state.range(0); i++) {
        c.add(i + 1);
        l.add(i + 1, rng());
    }

    for (auto _ : state) {
        Index i = c.committedIndex(l);
        benchmark::DoNotOptimize(i);
    }
}

BENCHMARK(BenchmarkMajorityConfigCommittedIndex)->DenseRange(1, 11, 2);

BENCHMARK_MAIN();