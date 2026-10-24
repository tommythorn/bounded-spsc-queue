use std::thread;

use bounded_spsc_queue::{make, make_cacheline};
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;

criterion_group!(
    benches,
    bench_single_thread_chan,
    bench_single_thread_spsc,
    bench_threaded_chan,
    bench_threaded_spsc,
    bench_threaded_reverse_chan,
    bench_threaded_reverse_spsc,
    bench_single_spsc_batch,
    bench_single_spsc_bulk_scalar,
    bench_single_spsc_skip,
    bench_cacheline_spsc,
    bench_cacheline_threaded_spsc,
    bench_cacheline_reverse_spsc
);
criterion_main!(benches);

fn bench_single_thread_chan(c: &mut Criterion) {
    c.bench_function("bench_single_chan", bench_chan);
}

fn bench_single_thread_spsc(c: &mut Criterion) {
    c.bench_function("bench_single_spsc", bench_spsc);
}

fn bench_threaded_chan(c: &mut Criterion) {
    c.bench_function("bench_threaded_chan", bench_chan_threaded);
}

fn bench_threaded_spsc(c: &mut Criterion) {
    c.bench_function("bench_threaded_spsc", bench_spsc_threaded);
}

fn bench_threaded_reverse_chan(c: &mut Criterion) {
    c.bench_function("bench_reverse_chan", bench_chan_threaded2);
}

fn bench_threaded_reverse_spsc(c: &mut Criterion) {
    c.bench_function("bench_reverse_spsc", bench_spsc_threaded2);
}

fn bench_single_spsc_batch(c: &mut Criterion) {
    c.bench_function("bench_single_spsc_batch", bench_spsc_batch);
}

fn bench_single_spsc_bulk_scalar(c: &mut Criterion) {
    c.bench_function("bench_single_spsc_bulk_scalar", bench_spsc_bulk_scalar);
}

fn bench_single_spsc_skip(c: &mut Criterion) {
    c.bench_function("bench_single_spsc_skip", bench_spsc_skip);
}

fn bench_cacheline_spsc(c: &mut Criterion) {
    c.bench_function("bench_cacheline_spsc", cacheline_spsc);
}

fn bench_cacheline_threaded_spsc(c: &mut Criterion) {
    c.bench_function("bench_cacheline_threaded_spsc", cacheline_threaded_spsc);
}

fn bench_cacheline_reverse_spsc(c: &mut Criterion) {
    c.bench_function("bench_cacheline_reverse_spsc", cacheline_reverse_spsc);
}

fn bench_chan(b: &mut Bencher) {
    let (tx, rx) = sync_channel::<u8>(500);
    b.iter(|| {
        tx.send(1).unwrap();
        black_box(rx.recv().unwrap())
    });
}

fn bench_chan_threaded(b: &mut Bencher) {
    let (tx, rx) = sync_channel::<u64>(500);
    let flag = AtomicBool::new(false);
    let arc_flag = Arc::new(flag);

    let flag_clone = arc_flag.clone();
    let handle = thread::spawn(move || {
        while !flag_clone.load(Ordering::Acquire) {
            // Try to do as much work as possible without checking the atomic
            for _ in 0..400 {
                rx.recv().unwrap();
            }
        }
    });

    b.iter(|| black_box(tx.send(1u64)));

    let flag_clone = arc_flag;
    flag_clone.store(true, Ordering::Release);

    // We have to loop a minimum of 400 times to guarantee the other thread shuts down
    for _ in 0..400 {
        let _ = tx.send(1);
    }

    handle.join().unwrap();
}

fn bench_chan_threaded2(b: &mut Bencher) {
    let (tx, rx) = sync_channel::<u8>(500);
    let flag = AtomicBool::new(false);
    let arc_flag = Arc::new(flag);

    let flag_clone = arc_flag.clone();
    let handle = thread::spawn(move || {
        while !flag_clone.load(Ordering::Acquire) {
            // Try to do as much work as possible without checking the atomic
            for _ in 0..400 {
                let _ = tx.send(1);
            }
        }
    });

    b.iter(|| black_box(rx.recv().unwrap()));

    let flag_clone = arc_flag;
    flag_clone.store(true, Ordering::Release);

    // We have to loop a minimum of 400 times to guarantee the other thread shuts down
    for _ in 0..400 {
        let _ = rx.try_recv();
    }

    handle.join().unwrap();
}

fn bench_spsc(b: &mut Bencher) {
    let (p, c) = make(500);

    b.iter(|| {
        p.push(1);
        black_box(c.pop())
    });
}

fn bench_spsc_threaded(b: &mut Bencher) {
    let (p, c) = make(500);

    let flag = AtomicBool::new(false);
    let arc_flag = Arc::new(flag);

    let flag_clone = arc_flag.clone();
    let handle = thread::spawn(move || {
        while !flag_clone.load(Ordering::Acquire) {
            // Try to do as much work as possible without checking the atomic
            for _ in 0..400 {
                c.pop();
            }
        }
    });

    b.iter(|| p.push(1));

    let flag_clone = arc_flag;
    flag_clone.store(true, Ordering::Release);

    // We have to loop a minimum of 400 times to guarantee the other thread shuts down
    for _ in 0..400 {
        p.try_push(1);
    }

    handle.join().unwrap();
}

fn bench_spsc_threaded2(b: &mut Bencher) {
    let (p, c) = make(500);

    let flag = AtomicBool::new(false);
    let arc_flag = Arc::new(flag);

    let flag_clone = arc_flag.clone();
    let handle = thread::spawn(move || {
        while !flag_clone.load(Ordering::Acquire) {
            // Try to do as much work as possible without checking the atomic
            for _ in 0..400 {
                p.push(1);
            }
        }
    });

    b.iter(|| black_box(c.pop()));

    let flag_clone = arc_flag;
    flag_clone.store(true, Ordering::Release);

    // We have to loop a minimum of 400 times to guarantee the other thread shuts down
    for _ in 0..400 {
        c.try_pop();
    }

    handle.join().unwrap();
}

fn bench_spsc_batch(b: &mut Bencher) {
    let (p, c) = make(500);
    let mut input: Vec<u32> = Vec::with_capacity(500);
    let mut output: Vec<u32> = Vec::with_capacity(500);

    b.iter(|| {
        input.clear();
        input.extend(0..500);
        black_box(p.try_push_from(&mut input));

        output.clear();
        assert_eq!(black_box(c.try_pop_n(&mut output, 500)), 500);
        black_box(output.len());
        assert_eq!(output.len(), 500);
    });
}

fn bench_spsc_bulk_scalar(b: &mut Bencher) {
    let (p, c) = make(500);
    let mut input: Vec<u32> = Vec::with_capacity(500);
    let mut output: Vec<u32> = Vec::with_capacity(500);

    b.iter(|| {
        input.clear();
        input.extend(0..500);
        for v in input.drain(..) {
            p.push(v);
        }

        output.clear();
        for _ in 0..500 {
            output.push(c.pop());
        }
        black_box(output.len());
        assert_eq!(output.len(), 500);
    });
}

fn bench_spsc_skip(b: &mut Bencher) {
    let (p, c) = make(500);

    b.iter(|| {
        for i in 0..500 {
            p.push(i);
        }
        assert_eq!(black_box(c.skip_n(500)), 500);
    });
}

fn cacheline_spsc(b: &mut Bencher) {
    let (p, c) = make_cacheline::<u64>(500);
    b.iter(|| {
        p.push(1u64);
        black_box(c.pop())
    });
}

fn cacheline_threaded_spsc(b: &mut Bencher) {
    let (p, c) = make_cacheline::<u64>(500);
    let flag = AtomicBool::new(false);
    let arc_flag = Arc::new(flag);

    let flag_clone = arc_flag.clone();
    let handle = thread::spawn(move || {
        while !flag_clone.load(Ordering::Acquire) {
            for _ in 0..400 {
                black_box(c.pop());
            }
        }
    });

    b.iter(|| p.push(1u64));

    let flag_clone = arc_flag;
    flag_clone.store(true, Ordering::Release);
    for _ in 0..400 {
        p.try_push(1u64);
    }
    handle.join().unwrap();
}

fn cacheline_reverse_spsc(b: &mut Bencher) {
    let (p, c) = make_cacheline::<u64>(500);
    let flag = AtomicBool::new(false);
    let arc_flag = Arc::new(flag);

    let flag_clone = arc_flag.clone();
    let handle = thread::spawn(move || {
        while !flag_clone.load(Ordering::Acquire) {
            for _ in 0..400 {
                p.push(1u64);
            }
        }
    });

    b.iter(|| black_box(c.pop()));

    let flag_clone = arc_flag;
    flag_clone.store(true, Ordering::Release);
    for _ in 0..1000 {
        c.try_pop();
    }
    handle.join().unwrap();
}
