use core::{mem, ptr};
use std::alloc;
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::CACHELINE_LEN;

#[cfg_attr(feature = "cacheline64", repr(C, align(64)))]
#[cfg_attr(feature = "cacheline128", repr(C, align(128)))]
#[cfg_attr(feature = "cacheline256", repr(C, align(256)))]
#[cfg_attr(
    all(
        not(feature = "cacheline64"),
        not(feature = "cacheline128"),
        not(feature = "cacheline256"),
        target_arch = "aarch64"
    ),
    repr(C, align(128))
)]
#[cfg_attr(
    all(
        not(feature = "cacheline64"),
        not(feature = "cacheline128"),
        not(feature = "cacheline256"),
        not(target_arch = "aarch64")
    ),
    repr(C, align(64))
)]
pub struct CachelineSlot<T> {
    payload: mem::MaybeUninit<T>,
    desc: AtomicU64,
}

#[inline]
fn slot_index(abs: usize, allocated_size: usize) -> usize {
    abs & (allocated_size - 1)
}

#[inline]
fn generation_phase(abs: usize, allocated_size: usize) -> u64 {
    ((abs >> allocated_size.trailing_zeros()) & 1) as u64
}

#[inline]
fn encode_desc(phase: u64, credit: usize) -> u64 {
    phase | ((credit as u64) << 1)
}

#[inline]
fn decode_desc(desc: u64) -> (u64, usize) {
    (desc & 1, (desc >> 1) as usize)
}

struct CachelineBuffer<T> {
    sq: *mut CachelineSlot<T>,
    cq: *mut CachelineSlot<()>,
    capacity: usize,
    allocated_size: usize,
    /// Absolute submission count published by the producer.
    sq_tail: Cell<usize>,
    /// Absolute submission count consumed by the consumer.
    sq_head: Cell<usize>,
}

unsafe impl<T: Sync> Sync for CachelineBuffer<T> {}

impl<T> CachelineBuffer<T> {
    /// Consume completion entries. Each CQE carries a saturating credit count:
    /// "there are at least C free submission slots". Credits are added to the
    /// producer's local credit budget and capped at capacity.
    fn drain_credits(&self, credit: &Cell<usize>, cq_head: &Cell<usize>) {
        let mut budget = credit.get();
        let mut cq = cq_head.get();
        loop {
            let expected = generation_phase(cq, self.allocated_size);
            let desc = unsafe {
                let idx = slot_index(cq, self.allocated_size);
                (*self.cq.add(idx)).desc.load(Ordering::Acquire)
            };
            if (desc & 1) != expected {
                break;
            }
            let (_, c) = decode_desc(desc);
            budget = budget.saturating_add(c).min(self.capacity);
            cq = cq.wrapping_add(1);
        }
        credit.set(budget);
        cq_head.set(cq);
    }

    #[inline]
    unsafe fn sq_slot(&self, abs: usize) -> &CachelineSlot<T> {
        let idx = slot_index(abs, self.allocated_size);
        &*self.sq.add(idx)
    }

    #[inline]
    unsafe fn cq_slot(&self, abs: usize) -> &CachelineSlot<()> {
        let idx = slot_index(abs, self.allocated_size);
        &*self.cq.add(idx)
    }
}

impl<T> Drop for CachelineBuffer<T> {
    fn drop(&mut self) {
        let head = self.sq_head.get();
        let tail = self.sq_tail.get();
        let mut cur = head;
        for _ in 0..tail.wrapping_sub(head) {
            let slot = unsafe { self.sq_slot(cur) };
            unsafe {
                drop(ptr::read(slot.payload.as_ptr()));
            }
            cur = cur.wrapping_add(1);
        }

        let sq_size = self
            .allocated_size
            .checked_mul(mem::size_of::<CachelineSlot<T>>())
            .expect("capacity overflow");
        let cq_size = self
            .allocated_size
            .checked_mul(mem::size_of::<CachelineSlot<()>>())
            .expect("capacity overflow");
        unsafe {
            let sq_layout = alloc::Layout::from_size_align(sq_size, CACHELINE_LEN).unwrap();
            let cq_layout = alloc::Layout::from_size_align(cq_size, CACHELINE_LEN).unwrap();
            alloc::dealloc(self.sq as *mut u8, sq_layout);
            alloc::dealloc(self.cq as *mut u8, cq_layout);
        }
    }
}

/// Producer half of a cache-line-aligned small-message queue.
pub struct CachelineProducer<T> {
    buffer: Arc<CachelineBuffer<T>>,
    credit: Cell<usize>,
    cq_head: Cell<usize>,
}

/// Consumer half of a cache-line-aligned small-message queue.
pub struct CachelineConsumer<T> {
    buffer: Arc<CachelineBuffer<T>>,
    cq_tail: Cell<usize>,
    pending_credits: Cell<usize>,
}

/// Completion entries are coalesced. A CQE is published only after this many
/// submissions have been consumed since the last CQE, or when the whole queue
/// capacity has been consumed for small queues.
const CREDIT_BATCH: usize = 64;

unsafe impl<T: Send> Send for CachelineProducer<T> {}
unsafe impl<T: Send> Send for CachelineConsumer<T> {}

impl<T> CachelineProducer<T> {
    #[inline]
    pub fn try_push(&self, v: T) -> Option<T> {
        let mut budget = self.credit.get();
        if budget == 0 {
            self.buffer.drain_credits(&self.credit, &self.cq_head);
            budget = self.credit.get();
            if budget == 0 {
                return Some(v);
            }
        }

        let sq_tail = self.buffer.sq_tail.get();
        let slot = unsafe { self.buffer.sq_slot(sq_tail) };
        let phase = generation_phase(sq_tail, self.buffer.allocated_size);
        // SQEs carry an advisory completion-space summary. Completion credits
        // are consumed only when the producer credit budget reaches zero, so
        // this field is not required for correctness.
        let desc = encode_desc(phase, self.buffer.capacity);
        unsafe {
            ptr::write(slot.payload.as_ptr() as *mut T, v);
            slot.desc.store(desc, Ordering::Release);
        }
        self.buffer.sq_tail.set(sq_tail.wrapping_add(1));
        self.credit.set(budget - 1);
        None
    }

    #[inline]
    pub fn push(&self, v: T) {
        let mut t = v;
        loop {
            match self.try_push(t) {
                Some(rv) => t = rv,
                None => return,
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity
    }

    /// Returns the producer's current view of queue occupancy.
    pub fn size(&self) -> usize {
        self.capacity() - self.credit.get()
    }

    pub fn free_space(&self) -> usize {
        self.credit.get()
    }
}

impl<T> CachelineConsumer<T> {
    #[inline]
    pub fn try_pop(&self) -> Option<T> {
        let sq_head = self.buffer.sq_head.get();
        let expected = generation_phase(sq_head, self.buffer.allocated_size);
        let slot = unsafe { self.buffer.sq_slot(sq_head) };
        let desc = slot.desc.load(Ordering::Acquire);
        if (desc & 1) != expected {
            return None;
        }

        let v = unsafe { ptr::read(slot.payload.as_ptr()) };
        self.buffer.sq_head.set(sq_head.wrapping_add(1));

        let pending = self.pending_credits.get() + 1;
        let batch = self.buffer.capacity.min(CREDIT_BATCH);
        if pending >= batch {
            let cq_tail = self.cq_tail.get();
            let cq_slot = unsafe { self.buffer.cq_slot(cq_tail) };
            let phase = generation_phase(cq_tail, self.buffer.allocated_size);
            // CQE summary: at least `pending` slots have been released in the SQ.
            let cqe_desc = encode_desc(phase, pending);
            cq_slot.desc.store(cqe_desc, Ordering::Release);
            self.cq_tail.set(cq_tail.wrapping_add(1));
            self.pending_credits.set(0);
        } else {
            self.pending_credits.set(pending);
        }
        Some(v)
    }

    #[inline]
    pub fn pop(&self) -> T {
        loop {
            match self.try_pop() {
                None => {}
                Some(v) => return v,
            }
        }
    }

    pub fn capacity(&self) -> usize {
        self.buffer.capacity
    }
}

/// Creates a cache-line-aligned small-message queue.
///
/// Each slot occupies an entire cache line. The payload must fit in the slot
/// after the descriptor word (`cacheline - 8` bytes on 64-bit targets). SQEs
/// carry a phase bit and a completion-space summary; CQEs carry a phase bit and
/// a submission-space credit summary. Completion credits are coalesced and are
/// not paired one-to-one with SQEs.
pub fn make_cacheline<T>(capacity: usize) -> (CachelineProducer<T>, CachelineConsumer<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    assert!(
        mem::align_of::<T>() <= CACHELINE_LEN,
        "payload alignment exceeds cache line size"
    );
    assert!(
        mem::offset_of!(CachelineSlot<T>, desc) + mem::size_of::<AtomicU64>() <= CACHELINE_LEN,
        "payload exceeds cache line slot size"
    );
    assert_eq!(
        mem::size_of::<CachelineSlot<T>>(),
        CACHELINE_LEN,
        "slot must occupy exactly one cache line"
    );

    let allocated_size = capacity
        .checked_next_power_of_two()
        .expect("capacity overflow");
    let sq_layout = alloc::Layout::from_size_align(
        allocated_size
            .checked_mul(mem::size_of::<CachelineSlot<T>>())
            .expect("capacity overflow"),
        CACHELINE_LEN,
    )
    .unwrap();
    let cq_layout = alloc::Layout::from_size_align(
        allocated_size
            .checked_mul(mem::size_of::<CachelineSlot<()>>())
            .expect("capacity overflow"),
        CACHELINE_LEN,
    )
    .unwrap();

    let sq = unsafe { alloc::alloc(sq_layout) as *mut CachelineSlot<T> };
    let cq = unsafe { alloc::alloc(cq_layout) as *mut CachelineSlot<()> };
    if sq.is_null() || cq.is_null() {
        unsafe {
            if !sq.is_null() {
                alloc::dealloc(sq as *mut u8, sq_layout);
            }
            if !cq.is_null() {
                alloc::dealloc(cq as *mut u8, cq_layout);
            }
        }
        alloc::handle_alloc_error(sq_layout);
    }

    // Initial phase is the opposite of generation zero, so an unwritten slot
    // never looks ready to the consumer.
    for i in 0..allocated_size {
        unsafe {
            (*sq.add(i)).desc.store(1, Ordering::Relaxed);
            (*cq.add(i)).desc.store(1, Ordering::Relaxed);
        }
    }

    let buffer = Arc::new(CachelineBuffer {
        sq,
        cq,
        capacity,
        allocated_size,
        sq_tail: Cell::new(0),
        sq_head: Cell::new(0),
    });

    (
        CachelineProducer {
            buffer: buffer.clone(),
            credit: Cell::new(capacity),
            cq_head: Cell::new(0),
        },
        CachelineConsumer {
            buffer,
            cq_tail: Cell::new(0),
            pending_credits: Cell::new(0),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    #[test]
    fn test_single_thread() {
        let (p, c) = make_cacheline::<u64>(10);
        for i in 0..10 {
            p.push(i);
        }
        for i in 0..10 {
            assert_eq!(c.pop(), i);
        }
        // The consumer coalesces its 10 consumed submissions into one CQE.
        assert_eq!(c.cq_tail.get(), 1);
    }

    #[test]
    fn test_empty_and_full() {
        let (p, c) = make_cacheline::<u64>(4);
        assert!(c.try_pop().is_none());

        for i in 0..4 {
            assert!(p.try_push(i).is_none());
        }
        let rejected = p.try_push(99).unwrap();
        assert_eq!(rejected, 99);

        // Consume half the queue; pending credits are not published yet because
        // the batch threshold is the full capacity.
        assert_eq!(c.pop(), 0);
        assert_eq!(c.pop(), 1);
        assert!(p.try_push(99).is_some());

        // Consuming the rest publishes a CQE and releases the SQ slots.
        assert_eq!(c.pop(), 2);
        assert_eq!(c.pop(), 3);
        assert!(p.try_push(99).is_none());
        assert_eq!(c.pop(), 99);
    }

    #[test]
    fn test_wrapping_generations() {
        let (p, c) = make_cacheline::<u32>(2);
        for round in 0..20u32 {
            p.push(round);
            assert_eq!(c.pop(), round);
            p.push(round + 100);
            assert_eq!(c.pop(), round + 100);
        }
    }

    #[test]
    fn test_phase_generations_non_power_of_two_capacity() {
        let (p, c) = make_cacheline::<u32>(3);
        for round in 0..50u32 {
            for i in 0..3 {
                p.push(round * 3 + i);
            }
            for i in 0..3 {
                assert_eq!(c.pop(), round * 3 + i);
            }
        }
    }

    #[test]
    fn test_small_capacity_one() {
        let (p, c) = make_cacheline::<u64>(1);
        for round in 0..20u64 {
            assert!(p.try_push(round).is_none());
            assert_eq!(c.pop(), round);
        }
    }

    #[test]
    fn test_credit_coalescing() {
        let (p, c) = make_cacheline::<u32>(128);
        for i in 0..64 {
            p.push(i);
        }
        for i in 0..63 {
            assert_eq!(c.pop(), i);
        }
        assert_eq!(c.cq_tail.get(), 0);
        assert_eq!(c.pending_credits.get(), 63);

        assert_eq!(c.pop(), 63);
        assert_eq!(c.cq_tail.get(), 1);
        assert_eq!(c.pending_credits.get(), 0);
    }

    #[test]
    fn test_credit_reuse_without_pairing() {
        let (p, c) = make_cacheline::<u64>(256);
        for cycle in 0..8u64 {
            for i in 0..256 {
                p.push(cycle * 256 + i);
            }
            for i in 0..256 {
                assert_eq!(c.pop(), cycle * 256 + i);
            }
        }
    }

    #[test]
    fn test_non_copy_payload() {
        let (p, c) = make_cacheline::<String>(4);
        let values = vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()];
        for v in values.clone() {
            p.push(v);
        }
        let mut out = Vec::new();
        for _ in 0..3 {
            out.push(c.pop());
        }
        assert_eq!(out, values);
        // Drop unconsumed payloads when the queue is dropped.
        drop(out);
        drop(p);
        drop(c);
    }

    struct TrackedDrop {
        counter: Arc<AtomicUsize>,
    }

    impl Drop for TrackedDrop {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_unconsumed_payloads_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (p, c) = make_cacheline::<TrackedDrop>(5);
        for _ in 0..5 {
            p.push(TrackedDrop {
                counter: counter.clone(),
            });
        }
        let a = c.pop();
        let b = c.pop();
        drop(a);
        drop(b);

        drop(p);
        drop(c);
        assert_eq!(counter.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_threaded_stress() {
        let (p, c) = make_cacheline::<u64>(500);
        thread::spawn(move || {
            for i in 0..100000 {
                p.push(i);
            }
        });
        for i in 0..100000 {
            assert_eq!(c.pop(), i);
        }
    }

    #[test]
    fn test_threaded_small_capacity() {
        let (p, c) = make_cacheline::<u64>(8);
        thread::spawn(move || {
            for i in 0..10000 {
                p.push(i);
            }
        });
        for i in 0..10000 {
            assert_eq!(c.pop(), i);
        }
    }

    #[test]
    fn test_try_push_pop_threaded() {
        let (p, c) = make_cacheline::<u32>(64);
        thread::spawn(move || {
            for i in 0..20000u32 {
                p.push(i);
            }
        });
        for i in 0..20000u32 {
            assert_eq!(c.pop(), i);
        }
    }
}
