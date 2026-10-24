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
fn encode_desc(phase: u64, space: usize) -> u64 {
    ((space as u64) << 1) | phase
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
    /// Consume completion credits from the credit queue. Each consumed credit
    /// releases one submission slot.
    fn drain_credits(&self, sq_head: &Cell<usize>, cq_head: &Cell<usize>) {
        let mut sq = sq_head.get();
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
            sq = sq.wrapping_add(1);
            cq = cq.wrapping_add(1);
        }
        sq_head.set(sq);
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
    sq_head: Cell<usize>,
    cq_head: Cell<usize>,
}

/// Consumer half of a cache-line-aligned small-message queue.
pub struct CachelineConsumer<T> {
    buffer: Arc<CachelineBuffer<T>>,
    cq_tail: Cell<usize>,
}

unsafe impl<T: Send> Send for CachelineProducer<T> {}
unsafe impl<T: Send> Send for CachelineConsumer<T> {}

impl<T> CachelineProducer<T> {
    #[inline]
    pub fn try_push(&self, v: T) -> Option<T> {
        let sq_tail = self.buffer.sq_tail.get();
        let mut sq_head = self.sq_head.get();
        if sq_tail.wrapping_sub(sq_head) >= self.buffer.capacity {
            self.buffer.drain_credits(&self.sq_head, &self.cq_head);
            sq_head = self.sq_head.get();
            if sq_tail.wrapping_sub(sq_head) >= self.buffer.capacity {
                return Some(v);
            }
        }

        let slot = unsafe { self.buffer.sq_slot(sq_tail) };
        let phase = generation_phase(sq_tail, self.buffer.allocated_size);
        // Completion space is advisory metadata. The producer consumes credits
        // only when the submission queue fills, so advertise the full credit
        // queue rather than forcing a scan on every push.
        let desc = encode_desc(phase, self.buffer.capacity);
        unsafe {
            ptr::write(slot.payload.as_ptr() as *mut T, v);
            slot.desc.store(desc, Ordering::Release);
        }
        self.buffer.sq_tail.set(sq_tail.wrapping_add(1));
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

    pub fn size(&self) -> usize {
        let tail = self.buffer.sq_tail.get();
        tail.wrapping_sub(self.sq_head.get())
    }

    pub fn free_space(&self) -> usize {
        self.capacity() - self.size()
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
        // A zero space hint means the producer has no completion room; wait for
        // the producer to consume credits before claiming this submission.
        if (desc >> 1) == 0 {
            return None;
        }

        let v = unsafe { ptr::read(slot.payload.as_ptr()) };

        let cq_tail = self.cq_tail.get();
        let cq_slot = unsafe { self.buffer.cq_slot(cq_tail) };
        let phase = generation_phase(cq_tail, self.buffer.allocated_size);
        // Consumer advertises the submission queue space it can see. The value
        // is advisory metadata; the producer still tracks credits itself.
        let credit = encode_desc(phase, self.buffer.capacity);
        cq_slot.desc.store(credit, Ordering::Release);
        self.cq_tail.set(cq_tail.wrapping_add(1));
        self.buffer.sq_head.set(sq_head.wrapping_add(1));
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
/// after the descriptor word (`cacheline - 8` bytes on 64-bit targets). The
/// producer publishes submissions, the consumer publishes completion credits,
/// and the producer consumes those credits to release submission slots.
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
            sq_head: Cell::new(0),
            cq_head: Cell::new(0),
        },
        CachelineConsumer {
            buffer,
            cq_tail: Cell::new(0),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_threaded() {
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
}
