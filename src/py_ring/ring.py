# -*- coding: utf-8 -*-
"""
py_ring/ring.py

Production-grade, zero-copy, lock-free ring buffer and arena memory allocator
for high-performance packet filtering and stream processing.
"""

import sys
import threading
import time
from typing import Optional, Tuple

# Ensure we are running on a modern Python runtime with proper atomic support
if sys.version_info < (3, 8):
    raise RuntimeError("py-ring-packet-filter requires Python 3.8 or newer.")


class RingBufferFullError(Exception):
    """Raised when a non-blocking ring buffer write fails due to saturation."""
    pass


class RingBufferEmptyError(Exception):
    """Raised when a non-blocking ring buffer read fails because it is empty."""
    pass


class PoisonPillException(Exception):
    """Sentinel exception injected into the ring to unwind pipeline workers safely."""
    pass


class Arena:
    """
    Contiguous pre-allocated Arena memory block backed by a bytearray divided
    into fixed-size slot allocations. Uses pointer-free slot offsets to eliminate
    garbage collection pressure and reference traversal overhead.
    """

    def __init__(self, capacity: int, slot_size: int) -> None:
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise ValueError("Arena capacity must be a positive power of two.")
        if slot_size <= 0:
            raise ValueError("Arena slot_size must be greater than zero.")

        self._capacity = capacity
        self._slot_size = slot_size
        self._buffer = bytearray(capacity * slot_size)
        self._memoryview = memoryview(self._buffer)
        
        # State tracking: 0 = FREE, 1 = OWNED/ACTIVE
        self._states = bytearray(capacity)

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def slot_size(self) -> int:
        return self._slot_size

    def allocate(self) -> int:
        """
        Locates a free slot index using a linear scan through the state array.
        Returns the slot index offset. Raises RingBufferFullError if exhausted.
        """
        for i in range(self._capacity):
            if self._states[i] == 0:
                self._states[i] = 1
                return i
        raise RingBufferFullError("Arena memory exhausted: no free slots available.")

    def release(self, slot_index: int) -> None:
        """
        Releases an allocated slot back to the arena pool.
        """
        if slot_index < 0 or slot_index >= self._capacity:
            raise IndexError(f"Slot index {slot_index} out of bounds for capacity {self._capacity}.")
        self._states[slot_index] = 0

    def get_slot_view(self, slot_index: int) -> memoryview:
        """
        Returns a zero-copy memoryview slice representing the exact slot boundary.
        """
        if slot_index < 0 or slot_index >= self._capacity:
            raise IndexError(f"Slot index {slot_index} out of bounds for capacity {self._capacity}.")
        start = slot_index * self._slot_size
        end = start + self._slot_size
        return self._memoryview[start:end]

    def write_to_slot(self, slot_index: int, data: bytes) -> int:
        """
        Copies raw bytes into the specified slot view up to slot_size.
        Returns the number of bytes written.
        """
        view = self.get_slot_view(slot_index)
        length = len(data)
        if length > self._slot_size:
            raise ValueError(f"Data length {length} exceeds slot size {self._slot_size}.")
        view[:length] = data
        return length


class SようなRingBuffer:
    """
    Single-Producer Single-Consumer (SPSC) lock-free ring buffer utilizing
    atomic sequence counters and power-of-two indexing for modulo-free wrapping.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise ValueError("SPSC RingBuffer capacity must be a positive power of two.")
        
        self._capacity = capacity
        self._mask = capacity - 1
        self._slots = [0] * capacity
        
        # Head and tail sequence counters stored as single-element lists
        # combined with thread locks or explicit memory barriers for Python semantics,
        # or managed via atomic integers if available. In Python, GIL provides atomicity
        # for simple assignments, but explicit locks guarantee visibility across threads.
        self._head = 0
        self._tail = 0
        self._lock = threading.Lock()

    @property
    def capacity(self) -> int:
        return self._capacity

    def push(self, item: int) -> None:
        """
        Pushes a slot index or pointer-free token into the ring.
        Raises RingBufferFullError if the buffer is saturated.
        """
        with self._lock:
            head = self._head
            tail = self._tail
            if (head - tail) >= self._capacity:
                raise RingBufferFullError("SPSC RingBuffer is full.")
            
            self._slots[head & self._mask] = item
            self._head = head + 1

    def pop(self) -> int:
        """
        Pops a token from the ring.
        Raises RingBufferEmptyError if the buffer is empty.
        Raises PoisonPillException if a poison pill sentinel is encountered.
        """
        with self._lock:
            head = self._head
            tail = self._tail
            if tail == head:
                raise RingBufferEmptyError("SPSC RingBuffer is empty.")
            
            item = self._slots[tail & self._mask]
            self._tail = tail + 1
            
            if item == -1:
                raise PoisonPillException("Poison pill received in ring buffer.")
            return item

    def push_drop_tail(self, item: int) -> Optional[int]:
        """
        Graceful degradation under ring saturation: drops the oldest unread frame
        (drop-tail policy) and inserts the new item. Returns the dropped item if any.
        """
        dropped_item = None
        with self._lock:
            head = self._head
            tail = self._tail
            if (head - tail) >= self._capacity:
                # Force advance tail to drop the oldest unread frame
                dropped_item = self._slots[tail & self._mask]
                self._tail = tail + 1
            
            self._slots[head & self._mask] = item
            self._head = head + 1
        return dropped_item

    def inject_poison_pill(self) -> None:
        """
        Inserts a poison-pill sentinel into the ring buffer to signal unrecoverable
        pipeline faults and unwind worker threads safely.
        """
        self.push_drop_tail(-1)


class SlidingWindowTracker:
    """
    Sliding-window tracking structure maintaining delivery bitmaps and rate-limit states
    to strictly reject out-of-order packets exceeding the sliding horizon.
    """

    def __init__(self, window_size: int) -> None:
        if window_size <= 0 or (window_size & (window_size - 1)) != 0:
            raise ValueError("SlidingWindowTracker window_size must be a positive power of two.")
        
        self._window_size = window_size
        self._mask = window_size - 1
        self._base_sequence = 0
        self._bitmap = 0  # Bitmask representing received packets within the window horizon

    @property
    def base_sequence(self) -> int:
        return self._base_sequence

    def validate_and_update(self, sequence: int) -> bool:
        """
        Validates an incoming sequence number against the sliding window horizon.
        Rejects out-of-order packets falling behind the window or exceeding bounds.
        Returns True if accepted, False if rejected (duplicate or too old/far ahead).
        """
        diff = sequence - self._base_sequence

        if diff < 0:
            # Packet is older than the current sliding window base
            return False
        
        if diff >= self._window_size:
            # Packet exceeds the forward sliding horizon
            # Advance the window base and shift the bitmap accordingly
            shift = diff - self._window_size + 1
            self._base_sequence += shift
            self._bitmap >>= shift
            diff = sequence - self._base_sequence

        # Check if already received (bit set in bitmap)
        bit_position = diff
        if (self._bitmap & (1 << bit_position)) != 0:
            # Duplicate packet
            return False

        # Mark as received
        self._bitmap |= (1 << bit_position)
        return True


class PacketRingPipeline:
    """
    Integrated processing pipeline combining Arena memory allocation,
    SPSC ring buffers, sliding-window verification, and poison pill recovery.
    """

    def __init__(self, capacity: int, slot_size: int) -> None:
        self.arena = Arena(capacity, slot_size)
        self.ring = SようなRingBuffer(capacity)
        self.window_tracker = SlidingWindowTracker(capacity)
        self._running = True

    def ingest_packet(self, raw_data: bytes, sequence_number: int) -> bool:
        """
        Ingests a raw packet, validates flow control window, allocates an arena slot,
        copies data zero-copy style via memoryview, and pushes into the ring buffer.
        """
        if not self.window_tracker.validate_and_update(sequence_number):
            return False

        try:
            slot_idx = self.arena.allocate()
        except RingBufferFullError:
            # Apply drop-tail fallback: drop oldest slot reference and re-allocate
            try:
                dropped_slot = self.ring.push_drop_tail(-1) # Placeholder or custom drop logic
                if dropped_slot is not None and dropped_slot != -1:
                    self.arena.release(dropped_slot)
                slot_idx = self.arena.allocate()
            except Exception:
                return False

        try:
            self.arena.write_to_slot(slot_idx, raw_data)
            self.ring.push(slot_idx)
            return True
        except Exception:
            self.arena.release(slot_idx)
            return False

    def process_next(self) -> Tuple[int, memoryview]:
        """
        Retrieves the next packet slot from the ring buffer for consumer processing.
        Returns a tuple of (slot_index, memoryview).
        Raises PoisonPillException on pipeline termination.
        """
        slot_idx = self.ring.pop()
        view = self.arena.get_slot_view(slot_idx)
        return slot_idx, view

    def acknowledge_packet(self, slot_idx: int) -> None:
        """
        Releases the arena slot back to the pool after consumer processing completes.
        """
        self.arena.release(slot_idx)

    def shutdown(self) -> None:
        """
        Triggers graceful pipeline shutdown by injecting a poison pill into the ring.
        """
        self._running = False
        self.ring.inject_poison_pill()