use std::thread;

use bounded_spsc_queue::make;
use criterion::{criterion_group, criterion_main, Bencher, Criterion};
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
    bench_threaded_reverse_spsc
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

fn bench_chan(b: &mut Bencher) {
    let (tx, rx) = sync_channel::<u8>(500);
    b.iter(|| {
        tx.send(1).unwrap();
        rx.recv().unwrap()
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

    b.iter(|| tx.send(1u64));

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

    b.iter(|| rx.recv().unwrap());

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
        c.pop()
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

    b.iter(|| c.pop());

    let flag_clone = arc_flag;
    flag_clone.store(true, Ordering::Release);

    // We have to loop a minimum of 400 times to guarantee the other thread shuts down
    for _ in 0..400 {
        c.try_pop();
    }

    handle.join().unwrap();
}
