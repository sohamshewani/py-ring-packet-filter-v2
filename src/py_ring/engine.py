import os
import sys
import time
import socket
import select
import struct
import threading
import selectors
from typing import Optional, Tuple, List, Union, Callable

class RingBufferException(Exception):
    """Base exception for ring buffer and engine faults."""
    pass

class RingBufferFullError(RingBufferException):
    """Raised when ring buffer is saturated and drop-tail is disabled."""
    pass

class RingBufferEmptyError(RingBufferException):
    """Raised when ring buffer has no readable items."""
    pass

class PoisonPillException(RingBufferException):
    """Raised when a poison pill sentinel is encountered in the ring."""
    pass


class PacketArena:
    """
    Contiguous pre-allocated Arena memory block backed by a bytearray,
    divided into fixed-size slot allocations using pointer-free slot offsets
    to eliminate garbage collection pressure and reference traversal overhead.
    """
    def __init__(self, capacity: int, slot_size: int) -> None:
        self.capacity = capacity
        self.slot_size = slot_size
        self.total_size = capacity * slot_size
        self._buffer = bytearray(self.total_size)
        # State tracking: 0 = FREE, 1 = OWNED/ACTIVE, 2 = COMMITTED
        self._states = bytearray(capacity)
        self._lock = threading.Lock()

    def acquire_slot(self) -> int:
        """Scan for a free slot and mark it owned. Returns slot index."""
        with self._lock:
            for i in range(self.capacity):
                if self._states[i] == 0:
                    self._states[i] = 1
                    return i
        raise RingBufferFullError("PacketArena is exhausted of free slots.")

    def release_slot(self, index: int) -> None:
        """Mark a slot as free."""
        with self._lock:
            self._states[index] = 0

    def get_memoryview(self, index: int) -> memoryview:
        """Return a zero-copy memoryview slice for the given slot index."""
        start = index * self.slot_size
        end = start + self.slot_size
        return memoryview(self._buffer)[start:end]

    def write_packet(self, index: int, data: bytes) -> int:
        """Write raw bytes into the slot memoryview, returning length written."""
        mv = self.get_memoryview(index)
        length = len(data)
        if length > self.slot_size:
            raise ValueError(f"Packet size {length} exceeds slot size {self.slot_size}")
        mv[:length] = data
        return length


class SpscAtoMicRingBuffer:
    """
    Lock-free Single-Producer Single-Consumer (SPSC) atomic sequence-based
    circular ring buffer using power-of-two indexing for modulo-free wrapping.
    """
    def __init__(self, capacity: int) -> None:
        if (capacity & (capacity - 1)) != 0 or capacity == 0:
            raise ValueError("Capacity must be a non-zero power of two.")
        self.capacity = capacity
        self.mask = capacity - 1
        self.slots: List[int] = [-1] * capacity
        self._head = 0  # Produced index
        self._tail = 0  # Consumed index
        self._producer_cache = 0
        self._consumer_cache = 0
        self._lock = threading.Lock() # Guard for strict SPSC atomicity if cross-thread visibility needs explicit fences, though Python uses GIL. We use atomic increments via explicit state.

    def push(self, slot_index: int, drop_tail: bool = True) -> None:
        head = self._head
        tail = self._consumer_cache

        if (head - tail) >= self.capacity:
            if drop_tail:
                # Force advance tail to drop oldest frame (graceful degradation)
                self._tail = (self._tail + 1) & self.mask
                tail = self._tail
            else:
                raise RingBufferFullError("SPSC Ring Buffer is full.")

        self.slots[head & self.mask] = slot_index
        # Memory barrier / atomic publication
        self._head = head + 1

    def pop(self) -> int:
        tail = self._tail
        head = self._producer_cache

        if tail == self._head:
            # Refresh head cache
            self._producer_cache = self._head
            head = self._producer_cache
            if tail == head:
                raise RingBufferEmptyError("SPSC Ring Buffer is empty.")

        slot_index = self.slots[tail & self.mask]
        self.slots[tail & self.mask] = -1
        self._tail = tail + 1
        
        if slot_index == -2:
            raise PoisonPillException("Poison pill detected in SPSC ring buffer.")
            
        return slot_index

    def push_poison_pill(self) -> None:
        head = self._head
        self.slots[head & self.mask] = -2
        self._head = head + 1


class SlidingWindowTracker:
    """
    Sliding-window tracking structures maintaining delivery bitmaps
    and rate-limit states, strictly rejecting out-of-order packets exceeding the horizon.
    """
    def __init__(self, window_size: int = 64, rate_limit_bps: int = 1000000) -> None:
        self.window_size = window_size
        self.rate_limit_bps = rate_limit_bps
        self.base_sequence = 0
        self.bitmap = 0
        self._tokens = float(rate_limit_bps)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def validate_and_update(self, sequence: int, packet_size: int) -> bool:
        with self._lock:
            # Rate limiting token bucket check
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._last_refill = now
            self._tokens = min(float(self.rate_limit_bps), self._tokens + elapsed * self.rate_limit_bps)

            required_tokens = packet_size * 8
            if self._tokens < required_tokens:
                # Rate limit exceeded
                return False
            self._tokens -= required_tokens

            # Sliding window horizon check
            if sequence < self.base_sequence:
                # Too old, out of window
                return False

            if sequence >= self.base_sequence + self.window_size:
                # Exceeds sliding horizon, reject out-of-order
                shift = sequence - (self.base_sequence + self.window_size - 1)
                self.base_sequence += shift
                self.bitmap >>= shift
                if sequence >= self.base_sequence + self.window_size:
                    return False

            bit_offset = sequence - self.base_sequence
            if (self.bitmap & (1 << bit_offset)) != 0:
                # Duplicate packet
                return False

            # Mark as received
            self.bitmap |= (1 << bit_offset)
            return True


class PacketFilterEngine:
    """
    Production Engine implementing non-blocking vectorized poll loops,
    arena memory management, SPSC ring pipelines, and failure recovery modes.
    """
    def __init__(self, capacity: int = 1024, slot_size: int = 2048, drop_tail: bool = True) -> None:
        self.capacity = capacity
        self.slot_size = slot_size
        self.drop_tail = drop_tail

        self.arena = PacketArena(capacity, slot_size)
        self.ring = SpscAtoMicRingBuffer(capacity)
        self.window_tracker = SlidingWindowTracker()

        self.sel = selectors.DefaultSelector()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self.packet_counter = 0
        self.dropped_counter = 0

    def register_socket(self, sock: socket.socket) -> None:
        sock.setblocking(False)
        self.sel.register(sock, selectors.EVENT_READ, data=self._handle_socket_read)

    def unregister_socket(self, sock: socket.socket) -> None:
        self.sel.unregister(sock)

    def _handle_socket_read(self, sock: socket.socket) -> None:
        try:
            slot_idx = self.arena.acquire_slot()
        except RingBufferFullError:
            self.dropped_counter += 1
            # Read and discard to clear socket buffer if arena is full
            try:
                sock.recv(self.slot_size)
            except BlockingIOError:
                pass
            return

        mv = self.arena.get_memoryview(slot_idx)
        try:
            nbytes = sock.recv_into(mv)
            if nbytes == 0:
                self.arena.release_slot(slot_idx)
                return

            # Example protocol parsing: first 8 bytes = sequence number (big-endian uint64)
            if nbytes >= 8:
                seq = struct.unpack("!Q", mv[:8].tobytes())[0]
            else:
                seq = self.packet_counter

            self.packet_counter += 1

            # Validate against sliding window rate limiter
            if not self.window_tracker.validate_and_update(seq, nbytes):
                self.arena.release_slot(slot_idx)
                self.dropped_counter += 1
                return

            # Push to SPSC ring buffer
            self.ring.push(slot_idx, drop_tail=self.drop_tail)

        except BlockingIOError:
            self.arena.release_slot(slot_idx)
        except Exception:
            self.arena.release_slot(slot_idx)
            raise

    def worker_loop(self) -> None:
        """Pipeline worker thread processing packets from the ring buffer."""
        while self._running:
            try:
                slot_idx = self.ring.pop()
            except RingBufferEmptyError:
                time.sleep(0.0001)  # Yield CPU briefly under empty conditions
                continue
            except PoisonPillException:
                # Unwind worker thread safely
                break

            try:
                mv = self.arena.get_memoryview(slot_idx)
                # Perform zero-copy packet filtering/inspection logic here
                # e.g. Inspecting payload or headers directly via memoryview
                _ = mv[0:16] # Access to validate memory layout zero-copy
            finally:
                self.arena.release_slot(slot_idx)

    def start(self) -> None:
        self._running = True
        self._worker_thread = threading.Thread(target=self.worker_loop, daemon=True)
        self._worker_thread.start()

    def poll_once(self, timeout: float = 0.01) -> None:
        events = self.sel.select(timeout=timeout)
        for key, mask in events:
            callback = key.data
            callback(key.fileobj)

    def run_poll_loop(self) -> None:
        while self._running:
            try:
                self.poll_once(timeout=0.01)
            except Exception:
                if not self._running:
                    break
                raise

    def stop(self) -> None:
        self._running = False
        # Insert poison pill sentinel to wake up and unwind worker thread
        try:
            self.ring.push_poison_pill()
        except Exception:
            pass

        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=1.0)

        self.sel.close()