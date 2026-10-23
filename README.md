
## Bounded SPSC Queue

[![Nightly Build Status](https://travis-ci.org/polyfractal/bounded-spsc-queue.svg?branch=master)](https://travis-ci.org/polyfractal/bounded-spsc-queue)

This crate provides a very simple bounded, Single-producer Single-consumer (SPSC)
queue for Rust.  It provides a data structure for two threads to communicate
in one direction with minimal overhead and bounded semantics.

Compared to a `sync_channel`, this queue provides a small but consistent
speedup.  `sync_channel` utilizes an unbounded linked-list data structure under the covers,
while `bounded_spsc_queue` is a simple ring buffer with single, solid block of allocated
memory.  The solid block of memory allows better cache pre-fetching due to less pointer
indirection, and generally simpler operations to achieve a bounded SPSC queue.

## Documentation

[Documentation can be found here](http://polyfractal.github.io/bounded-spsc-queue/bounded_spsc_queue/index.html)

## Example

```rust
use std::thread;
use bounded_spsc_queue::make;

// Initialize a queue with capacity of 500 values
let (p, c) = make(500);

// Spawn a new thread and move the Producer into it
thread::spawn(move|| {
  for i in 0..100000 {
    p.push(i);
  }
});

// Back in the first thread, start pop'ing values off the queue
for i in 0..100000 {
  let t = c.pop();
  assert!(t == i);
}
```

## Semi-scientific Benchmarks
On my Macbook Air (dual-core 1.7 GHz Intel Core i7), `sync_channel` can perform
single threaded `send()/recv()` paired operations in ~200ns.  

![Sync Chan PDF](/sync_chan_pdf.png)

`spsc` performs the same test in ~10ns.
![SPSC PDF](/spsc_pdf.png)

A more realistic, although harder test to accurately benchmark, is threaded
performance.  This test spins up a "listener" thread which attempts to drain
the queue as fast as possible.  It must intermittently check an Atomic flag
to determine if the test is over, which unfortunately will impact the benchmark results.
To help alleviate the impact, the listener will only check every 500 iterations.

In the original thread, the bencher attempts to push data onto the queue.
This is where timing can get flaky: if the listener is not draining fast enough,
the producer may stall while waiting for the queue to free a spot.  This test
should not be viewed as a latency test, but more as a throughput test since
it is really testing both producer and consumer performance in conjunction.

`sync_channel` scores ~6μs on this test:

![Sync Chan Threaded PDF](/sync_chan_threaded_pdf.png)

While `spsc` scores ~20ns:

![SPSC Threaded PDF](/spsc_threaded_pdf.png)

A second set of benchmarks look at the inverse (secondary thread pushes onto the
queue continuously, while the main thread pops data off).  The timings are very
similar.

Put another way:
- SPSC performs ~50m push operations per second
- Sync Channel performs ~170k operations per second

## Cache-Line Configuration

`Buffer<T>` aligns producer and consumer state to cache lines so the two sides
do not share a physical cache line. The default is 128 bytes on `aarch64`
(Apple Silicon) and 64 bytes elsewhere. The ideal value depends on the CPU:
the hardware cache-line size is not the only factor, because padding also
changes how much memory a queue touches.

The compile-time features `cacheline64`, `cacheline128`, and `cacheline256`
override the default. Enable exactly one:

```sh
cargo build --features cacheline128
cargo bench --features cacheline128
```

Use `cacheline64` for CPUs where larger padding regresses the threaded
benchmarks, `cacheline128` where it helps, and `cacheline256` only after
measuring both threaded directions.

## Platform Findings

Measured with Criterion 0.8.2 on release builds, capacity 500. Values are ns per
push/pop operation; bulk rows are one 500-element push+pop burst (also ns).
Threaded numbers are throughput-oriented and are noticeably noisy, especially
under CPU frequency scaling and thread scheduling.

| Platform | CPU | HW cache line | Padding | Single SPSC | Threaded SPSC | Reverse SPSC | Bulk push/pop | Bulk scalar | Bulk skip | Recommendation |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| coffee | AMD Ryzen 9 9950X3D (Zen 5) | 64 B | 64 B | 905.7 | 982.7 | 2.035 | 37.1 | 622.0 | 189.2 | `cacheline128` |
| coffee | AMD Ryzen 9 9950X3D (Zen 5) | 64 B | 128 B | 903.9 | 951.7 | 1.580 | 37.2 | 613.4 | 200.9 | reverse path improves consistently |
| coffee | AMD Ryzen 9 9950X3D (Zen 5) | 64 B | 256 B | 917.1 | 924.2 | 1.673 | 36.7 | 616.1 | 201.7 | sometimes fastest forward; reverse worse than 128 |
| sun | AMD EPYC 7443P (Zen 3) | 64 B | 64 B | 1788.3 | 3428.6 | 18.604 | 70.9 | 1246.4 | 431.2 | default `cacheline64` |
| sun | AMD EPYC 7443P (Zen 3) | 64 B | 128 B | 1786.3 | 4057.8 | 19.228 | 71.4 | 1264.2 | 433.6 | 128 B regresses threaded forward |
| mercury | Intel Core i9-13900KS (13th gen) | 64 B | 64 B | 1014.9 | 2085.5 | 12.319 | 36.9 | 604.6 | 256.9 | default `cacheline64` |
| mercury | Intel Core i9-13900KS (13th gen) | 64 B | 128 B | 1029.7 | 2134.3 | 17.820 | 37.9 | 605.8 | 237.5 | no consistent win; reverse noisy |
| mercury | Intel Core i9-13900KS (13th gen) | 64 B | 256 B | 1027.5 | 1943.1 | 19.363 | 37.8 | 606.6 | 254.7 | sometimes fastest forward; reverse worse |
| m5 | Apple M5 | 128 B | 64 B | 1753.4 | 1781.9 | 2.062 | 54.2 | 2570.0 | 815.6 | slightly better reverse, worse forward |
| m5 | Apple M5 | 128 B | 128 B | 1810.7 | 1708.3 | 2.127 | 53.6 | 2619.0 | 819.4 | default; best forward |
| m5 | Apple M5 | 128 B | 256 B | 1866.7 | 1719.2 | 2.103 | 53.5 | 2629.0 | 819.6 | no gain over 128 |

Takeaways:

- Zen 3 (`sun`) is happiest with 64-byte padding; 128-byte padding regresses
  the forward threaded path by ~18%.
- Zen 5 (`coffee`) benefits from 128-byte padding on the reverse path
  consistently (~1.58 ns vs ~2.0 ns with 64-byte). 256-byte padding can win the
  forward path but loses on the reverse path, so 128 is the safer choice.
- Intel 13th-gen (`mercury`) shows no consistent benefit from larger padding;
  keep the 64-byte default. Its reverse-threaded benchmark is noisy.
- Apple M5 uses 128-byte hardware cache lines and defaults to 128-byte padding.
  64-byte padding slightly helps reverse, but 128-byte is faster on the forward
  threaded path and bulk paths are effectively equal.
- Bulk APIs are roughly 10x faster than scalar loops on these machines; the
  exact ratio varies per platform.
