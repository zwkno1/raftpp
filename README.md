## Raft
Raft is a protocol with which a cluster of nodes can maintain a replicated state machine. 


This raft implementation is a full feature C++ implementation of Raft protocol. Features includes:

* Leader election
* Log replication
* Log compaction
* Membership changes

## Links for further research
* [Raft site](https://raft.github.io/)
* [Raft Paper](https://raft.github.io/raft.pdf)
* [Golang Implementaion](https://github.com/etcd-io/raft)
* [Rust Implementaion](https://github.com/tikv/raft-rs)

## Benchmarks
* raftpp
```shell
Running ./bench_quorum
Run on (12 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x12)
Load Average: 4.44, 5.01, 5.10
-----------------------------------------------------------------------------------
Benchmark                                         Time             CPU   Iterations
-----------------------------------------------------------------------------------
BenchmarkMajorityConfigCommittedIndex/1        4.29 ns         4.22 ns    165647148
BenchmarkMajorityConfigCommittedIndex/3        9.93 ns         9.78 ns     69679474
BenchmarkMajorityConfigCommittedIndex/5        24.8 ns         24.3 ns     29563183
BenchmarkMajorityConfigCommittedIndex/7        48.3 ns         46.6 ns     11798018
BenchmarkMajorityConfigCommittedIndex/9        54.3 ns         52.4 ns     10000000
BenchmarkMajorityConfigCommittedIndex/11       51.1 ns         50.2 ns     12293858
```

* etcd-io/raft
```shell
go test -bench=MajorityConfig_CommittedIndex
goos: darwin
goarch: arm64
pkg: go.etcd.io/raft/v3/quorum
BenchmarkMajorityConfig_CommittedIndex/voters=1-12          30572664            39.08 ns/op
BenchmarkMajorityConfig_CommittedIndex/voters=3-12          18253448            63.88 ns/op
BenchmarkMajorityConfig_CommittedIndex/voters=5-12          13233304            89.41 ns/op
BenchmarkMajorityConfig_CommittedIndex/voters=7-12          10670887           113.0 ns/op
BenchmarkMajorityConfig_CommittedIndex/voters=9-12           6226634           210.5 ns/op
BenchmarkMajorityConfig_CommittedIndex/voters=11-12          5045894           231.4 ns/op
```
* raft-rs
  
  bench code:
```rust
use std::time::Duration;

use criterion::Criterion;

type HashSet<K> = std::collections::HashSet<K, std::hash::BuildHasherDefault<fxhash::FxHasher>>;


use raft::{MajorityConfig, AckIndexer, Index};

pub fn bench_quorum(c: &mut Criterion) {
    for n in (1..13).step_by(2) {
        c.bench_function(&format!("BenchmarkMajorityConfigCommittedIndex/{}", n), move |b| {
            b.iter(||{
                let mut l = AckIndexer::new();
                let mut s : HashSet<u64> = HashSet::<u64>::default();
                for i in 0..n {
                    s.insert(i+1);
                    l.insert(i+1, Index{index: rand::random::<u64>(), group_id: 1});
                }
                let c = MajorityConfig::new(s);
                std::hint::black_box(c.committed_index(false, &l));
            })
        });
    }
}


fn main() {
    let mut c = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(1))
        .configure_from_args();
    bench_quorum(&mut c);

    c.final_summary();
}
```
bench result: 
```shell
     Running benches/benches.rs (target/release/deps/benches-e6ccd1adcf102544)
Gnuplot not found, using plotters backend
BenchmarkMajorityConfigCommittedIndex/1                                                                             
                        time:   [107.15 ns 107.42 ns 107.72 ns]
                        change: [-0.4849% +0.3250% +1.1183%] (p = 0.44 > 0.05)
                        No change in performance detected.
Found 15 outliers among 100 measurements (15.00%)
  2 (2.00%) low mild
  4 (4.00%) high mild
  9 (9.00%) high severe

BenchmarkMajorityConfigCommittedIndex/3                                                                             
                        time:   [243.19 ns 244.36 ns 245.58 ns]
                        change: [-1.2440% -0.6660% -0.1087%] (p = 0.03 < 0.05)
                        Change within noise threshold.

BenchmarkMajorityConfigCommittedIndex/5                                                                             
                        time:   [443.28 ns 445.03 ns 446.82 ns]
                        change: [-0.5971% +0.1513% +0.8623%] (p = 0.69 > 0.05)
                        No change in performance detected.
Found 2 outliers among 100 measurements (2.00%)
  2 (2.00%) high mild

BenchmarkMajorityConfigCommittedIndex/7                                                                             
                        time:   [595.15 ns 597.29 ns 599.41 ns]
                        change: [-1.5043% -0.9361% -0.3277%] (p = 0.00 < 0.05)
                        Change within noise threshold.
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe

BenchmarkMajorityConfigCommittedIndex/9                                                                             
                        time:   [918.38 ns 920.53 ns 922.74 ns]
                        change: [-0.0884% +0.5780% +1.2963%] (p = 0.09 > 0.05)
                        No change in performance detected.
Found 5 outliers among 100 measurements (5.00%)
  1 (1.00%) low mild
  3 (3.00%) high mild
  1 (1.00%) high severe

BenchmarkMajorityConfigCommittedIndex/11                                                                             
                        time:   [1.0183 µs 1.0216 µs 1.0248 µs]
                        change: [-0.4339% +0.1060% +0.7234%] (p = 0.71 > 0.05)
                        No change in performance detected.
Found 3 outliers among 100 measurements (3.00%)
  2 (2.00%) high mild
  1 (1.00%) high severe
```