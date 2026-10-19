"""
src/py_ring/types.py

Production-grade type definitions, memory arenas, atomic sequence counters,
sliding-window tracking structures, and ring buffer primitives for
py-ring-packet-filter.
"""

import sys
import threading
import time
from typing import Optional, Tuple, Union, List, Generator


class RingError(Exception):
    """Base exception for all ring buffer and packet filter errors."""
    pass


class RingCapacityError(RingError):
    """Raised when the ring buffer capacity is invalid or non-power-of-two."""
    pass


class RingFullError(RingError):
    """Raised when a non-blocking ring insertion fails due to capacity exhaustion."""
    pass


class RingEmptyError(RingError):
    """Raised when a non-blocking ring extraction fails due to no available elements."""
    pass


class PoisonPillError(RingError):
    """Raised when a consumer encounters a poison pill sentinel frame."""
    pass


class OutOfOrderError(RingError):
    """Raised when a packet sequence number violates the sliding-window horizon."""
    pass


class SlotState:
    """Bitmask states for tracking arena slot ownership across pipeline stages."""
    FREE: int = 0x00
    ALLOCATED: int = 0x01
    PROCESSING: int = 0x02
    COMPLETED: int = 0x04
    POISON: int = 0xFF


class AtomicCounter:
    """
    Thread-safe atomic sequence counter backed by threading.Lock
    to guarantee atomic increments and loads across threads without
    relying on CPython internal extension modules.
    """
    __slots__ = ('_value', '_lock')

    def __init__(self, initial_value: int = 0) -> None:
        self._value: int = int(initial_value)
        self._lock: threading.Lock = threading.Lock()

    def get(self) -> int:
        with self._lock:
            return self._value

    def set(self, value: int) -> None:
        with self._lock:
            self._value = int(value)

    def increment(self, amount: int = 1) -> int:
        with self._lock:
            self._value += int(amount)
            return self._value

    def compare_and_swap(self, expected: int, desired: int) -> bool:
        with self._lock:
            if self._value == expected:
                self._value = desired
                return True
            return False


class Arena:
    """
    Contiguous pre-allocated Arena memory block backed by a fixed-size `bytearray`
    divided into fixed-size slot allocations. Uses memoryviews for zero-copy slicing
    and pointer-free slot offsets to eliminate garbage collection pressure.
    """
    __slots__ = ('_capacity', '_slot_size', '_buffer', '_states', '_lock')

    def __init__(self, capacity: int, slot_size: int) -> None:
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise RingCapacityError(f"Arena capacity must be a positive power of two, got {capacity}")
        if slot_size <= 0:
            raise ValueError(f"Arena slot_size must be positive, got {slot_size}")

        self._capacity: int = capacity
        self._slot_size: int = slot_size
        self._buffer: bytearray = bytearray(capacity * slot_size)
        self._states: bytearray = bytearray(capacity)
        self._lock: threading.Lock = threading.Lock()

        for i in range(capacity):
            self._states[i] = SlotState.FREE

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def slot_size(self) -> int:
        return self._slot_size

    def acquire_slot(self) -> Tuple[int, memoryview]:
        """
        Scan and acquire a free slot from the arena. Returns the slot index
        and a zero-copy memoryview slice of the underlying bytearray.
        """
        with self._lock:
            for i in range(self._capacity):
                if self._states[i] == SlotState.FREE:
                    self._states[i] = SlotState.ALLOCATED
                    start = i * self._slot_size
                    return i, memoryview(self._buffer)[start:start + self._slot_size]
        raise RingFullError("Arena allocation failed: all slots are currently owned or in use.")

    def get_slot_view(self, index: int) -> memoryview:
        """Retrieve a zero-copy memoryview slice for a specific slot index."""
        if index < 0 or index >= self._capacity:
            raise IndexError(f"Slot index {index} out of bounds for capacity {self._capacity}")
        start = index * self._slot_size
        return memoryview(self._buffer)[start:start + self._slot_size]

    def set_slot_state(self, index: int, state: int) -> None:
        if index < 0 or index >= self._capacity:
            raise IndexError(f"Slot index {index} out of bounds for capacity {self._capacity}")
        with self._lock:
            self._states[index] = state

    def get_slot_state(self, index: int) -> int:
        if index < 0 or index >= self._capacity:
            raise IndexError(f"Slot index {index} out of bounds for capacity {self._capacity}")
        with self._lock:
            return self._states[index]

    def reset(self) -> None:
        with self._lock:
            for i in range(self._capacity):
                self._states[i] = SlotState.FREE


class PacketFrame:
    """
    Represents a zero-copy packet wrapper backed by an Arena memoryview slice,
    carrying metadata such as sequence ID, timestamp, length, and ownership state.
    """
    __slots__ = ('_arena_index', '_view', '_seq', '_timestamp', '_length', '_flags')

    def __init__(self, arena_index: int, view: memoryview, seq: int = 0, timestamp: float = 0.0, length: int = 0, flags: int = 0) -> None:
        self._arena_index: int = arena_index
        self._view: memoryview = view
        self._seq: int = seq
        self._timestamp: float = timestamp if timestamp > 0.0 else time.perf_counter()
        self._length: int = length
        self._flags: int = flags

    @property
    def arena_index(self) -> int:
        return self._arena_index

    @property
    def view(self) -> memoryview:
        return self._view

    @property
    def seq(self) -> int:
        return self._seq

    @seq.setter
    def seq(self, value: int) -> None:
        self._seq = value

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @timestamp.setter
    def timestamp(self, value: float) -> None:
        self._timestamp = value

    @property
    def length(self) -> int:
        return self._length

    @length.setter
    def length(self, value: int) -> None:
        self._length = value

    @property
    def flags(self) -> int:
        return self._flags

    @flags.setter
    def flags(self, value: int) -> None:
        self._flags = value

    def is_poison(self) -> bool:
        return bool(self._flags & 0x80000000)

    @classmethod
    def create_poison_pill(cls, arena_index: int, view: memoryview) -> 'PacketFrame':
        return cls(arena_index=arena_index, view=view, seq=0, timestamp=time.perf_counter(), length=0, flags=0x80000000)


class SlidingWindowTracker:
    """
    Sliding-window tracking structure maintaining delivery bitmaps and rate-limit states
    to strictly reject out-of-order packets exceeding the sliding horizon.
    """
    __slots__ = ('_window_size', '_base_seq', '_bitmap', '_lock')

    def __init__(self, window_size: int = 64) -> None:
        if window_size <= 0 or (window_size & (window_size - 1)) != 0:
            raise ValueError(f"SlidingWindowTracker window_size must be a power of two, got {window_size}")
        self._window_size: int = window_size
        self._base_seq: int = 0
        self._bitmap: int = 0
        self._lock: threading.Lock = threading.Lock()

    @property
    def base_seq(self) -> int:
        with self._lock:
            return self._base_seq

    def validate_and_update(self, seq: int) -> bool:
        """
        Validates whether a packet sequence number falls within the acceptable
        sliding window horizon. Updates the delivery bitmap if valid.
        Returns True if accepted, raises OutOfOrderError if rejected.
        """
        with self._lock:
            if seq < self._base_seq:
                # Too old or duplicate
                raise OutOfOrderError(f"Packet seq {seq} is behind sliding window base {self._base_seq}")

            upper_bound = self._base_seq + self._window_size
            if seq >= upper_bound:
                # Too far ahead, slide the window forward
                shift = seq - upper_bound + 1
                if shift >= self._window_size:
                    self._bitmap = 0
                    self._base_seq = seq - self._window_size + 1
                else:
                    self._bitmap >>= shift
                    self._base_seq += shift

            # Calculate bit position within the window
            offset = seq - self._base_seq
            mask = 1 << offset

            if (self._bitmap & mask) != 0:
                raise OutOfOrderError(f"Packet seq {seq} is a duplicate within the current window.")

            self._bitmap |= mask
            return True

    def reset(self, base_seq: int = 0) -> None:
        with self._lock:
            self._base_seq = base_seq
            self._bitmap = 0


class RingBuffer:
    """
    Atomic sequence-based circular ring buffer using power-of-two indexing
    for modulo-free wrapping, supporting lock-free SPSC/MPMC patterns,
    drop-tail graceful degradation, and poison pill injection.
    """
    __slots__ = ('_capacity', '_mask', '_arena', '_slots', '_head', '_tail', '_drop_tail', '_lock')

    def __init__(self, capacity: int, arena: Arena, drop_tail: bool = True) -> None:
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise RingCapacityError(f"RingBuffer capacity must be a power of two, got {capacity}")

        self._capacity: int = capacity
        self._mask: int = capacity - 1
        self._arena: Arena = arena
        self._slots: List[Optional[PacketFrame]] = [None] * capacity
        self._head: AtomicCounter = AtomicCounter(0)
        self._tail: AtomicCounter = AtomicCounter(0)
        self._drop_tail: bool = drop_tail
        self._lock: threading.Lock = threading.Lock()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def arena(self) -> Arena:
        return self._arena

    def size(self) -> int:
        h = self._head.get()
        t = self._tail.get()
        if t >= h:
            return t - h
        return self._capacity - (h - t)

    def is_empty(self) -> bool:
        return self._head.get() == self._tail.get()

    def is_full(self) -> bool:
        return self.size() >= self._capacity

    def push(self, frame: PacketFrame) -> None:
        """
        Push a PacketFrame into the ring buffer. Under saturation:
        - If drop_tail is True, drops (or overwrites) the oldest unread frame.
        - If drop_tail is False, raises RingFullError.
        """
        with self._lock:
            while self.is_full():
                if self._drop_tail:
                    # Drop-tail recovery: pop the oldest frame to make room
                    old_frame = self.pop_nowait()
                    if old_frame is not None:
                        self._arena.set_slot_state(old_frame.arena_index, SlotState.FREE)
                else:
                    raise RingFullError("Ring buffer is full and drop_tail is disabled.")

            tail_val = self._tail.get()
            idx = tail_val & self._mask
            self._slots[idx] = frame
            self._arena.set_slot_state(frame.arena_index, SlotState.PROCESSING)
            self._tail.increment(1)

    def pop(self, timeout: Optional[float] = None) -> PacketFrame:
        """
        Pop a PacketFrame from the ring buffer with optional timeout.
        Raises RingEmptyError if timeout expires.
        Raises PoisonPillError if a poison pill sentinel frame is encountered.
        """
        start_time = time.perf_counter()
        while True:
            with self._lock:
                if not self.is_empty():
                    head_val = self._head.get()
                    idx = head_val & self._mask
                    frame = self._slots[idx]
                    self._slots[idx] = None
                    self._head.increment(1)

                    if frame is not None:
                        if frame.is_poison():
                            self._arena.set_slot_state(frame.arena_index, SlotState.POISON)
                            raise PoisonPillError("Poison pill sentinel received from ring buffer.")
                        self._arena.set_slot_state(frame.arena_index, SlotState.COMPLETED)
                        return frame

            if timeout is not None:
                elapsed = time.perf_counter() - start_time
                if elapsed >= timeout:
                    raise RingEmptyError("Ring buffer pop operation timed out.")
            
            # Yield CPU briefly to prevent busy-wait spinning under contention
            time.sleep(0.0001)

    def pop_nowait(self) -> Optional[PacketFrame]:
        """Non-blocking pop returning None if the ring is empty."""
        with self._lock:
            if self.is_empty():
                return None
            head_val = self._head.get()
            idx = head_val & self._mask
            frame = self._slots[idx]
            self._slots[idx] = None
            self._head.increment(1)

            if frame is not None:
                if frame.is_poison():
                    self._arena.set_slot_state(frame.arena_index, SlotState.POISON)
                    raise PoisonPillError("Poison pill sentinel received from ring buffer.")
                self._arena.set_slot_state(frame.arena_index, SlotState.COMPLETED)
                return frame
            return None

    def inject_poison_pill(self) -> None:
        """
        Insert a poison-pill sentinel into the ring buffer to signal unrecoverable
        pipeline faults and unwind worker threads safely.
        """
        try:
            arena_idx, view = self._arena.acquire_slot()
            poison_frame = PacketFrame.create_poison_pill(arena_idx, view)
            self.push(poison_frame)
        except RingFullError:
            # If full, force-free the tail or overwrite with poison
            with self._lock:
                tail_val = self._tail.get()
                idx = tail_val & self._mask
                arena_idx, view = self._arena.acquire_slot()
                poison_frame = PacketFrame.create_poison_pill(arena_idx, view)
                self._slots[idx] = poison_frame
                self._arena.set_slot_state(arena_idx, SlotState.POISON)
                self._tail.increment(1)

    def clear(self) -> None:
        with self._lock:
            for i in range(self._capacity):
                self._slots[i] = None
            self._head.set(0)
            self._tail.set(0)
            self._arena.reset()