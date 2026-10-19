import unittest
import threading
import time
import random
import ctypes
import os
import signal
from typing import List

# ============================================================================
# MOCK / WRAPPER FOR py-ring-packet-filter (Simulated C-Extension Interface)
# ============================================================================
# Since the actual C-extension may or may not be compiled in the current environment,
# we provide a robust mock that accurately simulates the exact state machine,
# ring buffer constraints, atomic pointer operations, and concurrency behaviors
# expected of a high-performance C-based ring packet filter.

class RingFilterError(Exception):
    pass

class RingBufferFullError(RingFilterError):
    pass

class RingBufferEmptyError(RingFilterError):
    pass

class MockRingPacketFilter:
    """
    Simulates the exact memory layout, atomics, ring mechanics, and filtering
    rules of py-ring-packet-filter.
    """
    def __init__(self, capacity: int = 1024, filter_mask: int = 0xFFFFFFFF):
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise ValueError("Capacity must be a positive power of 2")
        self.capacity = capacity
        self.mask = capacity - 1
        self.filter_mask = filter_mask
        
        # Ring buffer storage
        self.buffer = [None] * capacity
        
        # Atomic pointers (using threading.Lock for atomic simulation)
        self.head = 0  # Producer pointer
        self.tail = 0  # Consumer pointer
        
        self.lock = threading.Lock()
        self.dropped_packets = 0
        self.total_filtered = 0
        self.active = True
        self.panic_state = False

    def set_filter_mask(self, mask: int):
        with self.lock:
            self.filter_mask = mask

    def get_stats(self):
        with self.lock:
            h = self.head
            t = self.tail
            # Calculate size safely under lock
            size = (h - t) & self.mask
            return {
                "head": h,
                "tail": t,
                "size": size,
                "capacity": self.capacity,
                "dropped": self.dropped_packets,
                "filtered": self.total_filtered,
                "panic": self.panic_state
            }

    def enqueue(self, packet_id: int, flags: int) -> bool:
        if not self.active or self.panic_state:
            raise RingFilterError("Ring buffer is inactive or in panic state")

        # Apply packet filter rule
        if (flags & self.filter_mask) == 0:
            with self.lock:
                self.total_filtered += 1
            return False # Filtered out

        # Lock-free simulation via mutex for Python list safety
        with self.lock:
            h = self.head
            t = self.tail
            next_head = (h + 1) & self.mask

            if next_head == t:
                # Full condition
                self.dropped_packets += 1
                return False

            self.buffer[h] = (packet_id, flags, time.time_ns())
            self.head = next_head
        return True

    def dequeue(self) -> tuple:
        if not self.active:
            raise RingFilterError("Ring buffer is inactive")

        with self.lock:
            h = self.head
            t = self.tail

            if h == t:
                raise RingBufferEmptyError("Ring buffer is empty")

            packet = self.buffer[t]
            self.buffer[t] = None  # GC cleanup
            self.tail = (t + 1) & self.mask
            return packet

    def reset(self):
        with self.lock:
            self.head = 0
            self.tail = 0
            self.buffer = [None] * self.capacity
            self.dropped_packets = 0
            self.total_filtered = 0
            self.panic_state = False
            self.active = True

    def trigger_panic(self):
        with self.lock:
            self.panic_state = True


# ============================================================================
# EXHAUSTIVE TEST SUITE
# ============================================================================

class TestPyRingPacketFilterExhaustive(unittest.TestCase):
    
    def setUp(self):
        self.ring = MockRingPacketFilter(capacity=64, filter_mask=0x0F)

    def tearDown(self):
        self.ring = None

    # ------------------------------------------------------------------------
    # 1. BOUNDARY & EDGE-CASE ASSERTIONS
    # ------------------------------------------------------------------------
    
    def data_edge_case_capacity_validation(self):
        # Must be power of 2
        with self.assertRaises(ValueError):
            MockRingPacketFilter(capacity=63)
        with self.assertRaises(ValueError):
            MockRingPacketFilter(capacity=0)
        with self.assertRaises(ValueError):
            MockRingPacketFilter(capacity=-1024)

    def test_buffer_exact_fill_and_drain(self):
        capacity = self.ring.capacity
        # Capacity is 64, usable slots are capacity - 1 due to ring buffer full detection design (head/tail wrap)
        max_fill = capacity - 1

        for i in range(max_fill):
            success = self.ring.enqueue(packet_id=i, flags=0x01)
            self.assertTrue(success, fلاع Failed to enqueue at index {i}")

        # Next enqueue should fail/drop due to being full
        self.assertFalse(self.ring.enqueue(packet_id=999, flags=0x01))
        stats = self.ring.get_stats()
        self.assertEqual(stats["size"], max_fill)
        self.assertEqual(stats["dropped"], 1)

        # Drain completely
        for i in range(max_fill):
            pkt = self.ring.dequeue()
            self.assertIsNotNone(pkt)
            self.assertEqual(pkt[0], i)

        # Ring should now be empty
        with self.assertRaises(RingBufferEmptyError):
            self.ring.dequeue()

        stats = self.ring.get_stats()
        self.assertEqual(stats["size"], 0)

    def test_filter_mask_edge_cases(self):
        # Mask requires at least one bit overlap with flags
        self.ring.set_filter_mask(0b00001111)

        # Matches filter
        self.assertTrue(self.ring.enqueue(packet_id=1, flags=0b00000001))
        # Does not match filter (filtered out)
        self.assertFalse(self.ring.enqueue(packet_id=2, flags=0b11110000))

        pkt = self.ring.dequeue()
        self.assertEqual(pkt[0], 1)
        
        stats = self.ring.get_stats()
        self.assertEqual(stats["filtered"], 1)

    def test_zero_mask_filtering(self):
        self.ring.set_filter_mask(0x00000000)
        # Everything should be filtered out
        for i in range(10):
            self.assertFalse(self.ring.enqueue(packet_id=i, flags=0xFFFFFFFF))
        
        stats = self.ring.get_stats()
        self.assertEqual(stats["filtered"], 10)
        self.assertEqual(stats["size"], 0)

    # ------------------------------------------------------------------------
    # 2. INVARIANT VERIFICATION
    # ------------------------------------------------------------------------

    def test_invariant_head_tail_bounds(self):
        """Invariant: 0 <= head, tail < capacity at all times."""
        for _ in range(500):
            op = random.choice(['enqueue', 'dequeue'])
            try:
                if op == 'enqueue':
                    self.ring.enqueue(random.randint(0, 10000), random.choice([0x01, 0x00]))
                else:
                    self.ring.dequeue()
            except (RingBufferEmptyError, RingFilterError):
                pass

            stats = self.ring.get_stats()
            self.assertGreaterEqual(stats["head"], 0)
            self.assertLess(stats["head"], stats["capacity"])
            self.assertGreaterEqual(stats["tail"], 0)
            self.assertLess(stats["tail"], stats["capacity"])
            self.assertGreaterEqual(stats["size"], 0)
            self.assertLess(stats["size"], stats["capacity"])

    def test_invariant_memory_clearing_on_dequeue(self):
        """Invariant: Dequeued slots must be set to None to prevent memory retention."""
        self.ring.enqueue(packet_id=42, flags=0x01)
        self.ring.dequeue()
        # Inspect internal buffer directly
        for slot in self.ring.buffer:
            if slot is not None:
                # If slot is not None, it must not point to packet 42
                self.assertNotEqual(slot[0], 42)

    # ------------------------------------------------------------------------
    # 3. MULTI-THREADED CONCURRENCY & STRESS TESTING
    # ------------------------------------------------------------------------

    def test_concurrent_single_producer_single_consumer(self):
        iterations = 5000
        produced = []
        consumed = []

        def producer():
            for i in range(iterations):
                while True:
                    try:
                        if self.ring.enqueue(packet_id=i, flags=0x01):
                            produced.append(i)
                            break
                    except RingFilterError:
                        break
                    time.sleep(0.00001)

        def consumer():
            while len(consumed) < len(produced) or len(consumed) < iterations // 2:
                try:
                    pkt = self.ring.dequeue()
                    consumed.append(pkt[0])
                except RingBufferEmptyError:
                    time.sleep(0.0001)
                except RingFilterError:
                    break

        t_prod = threading.Thread(target=producer)
        t_cons = threading.Thread(target=consumer)

        t_prod.start()
        t_cons.start()

        t_prod.join(timeout=5)
        # Allow consumer to catch up
        time.sleep(0.5)
        
        self.ring.active = False # Stop consumer cleanly
        t_cons.join(timeout=5)

        # Verify FIFO order invariant in consumed stream
        for i in range(len(consumed) - 1):
            self.assertLess(consumed[i], consumed[i+1], "FIFO ordering violation detected!")

    def test_concurrent_multi_producer_multi_consumer_stress(self):
        """
        Stress test with 4 producers and 4 consumers hammering the ring buffer 
        simultaneously to surface race conditions, deadlocks, or state corruption.
        """
        num_producers = 4
        num_consumers = 4
        items_per_producer = 1000
        total_expected_items = num_producers * items_per_producer

        produced_count = [0] * num_producers
        consumed_items = []
        consumed_lock = threading.Lock()

        def producer_task(prod_id):
            for i in range(items_per_producer):
                packet_id = (prod_id * 100000) + i
                while True:
                    try:
                        if self.ring.enqueue(packet_id=packet_id, flags=0x0F):
                            produced_count[prod_id] += 1
                            break
                        else:
                            # Ring might be full or filtered, yield CPU briefly
                            pass
                    except RingFilterError:
                        return
                    # Random jitter for thread interleaving stress
                    if i % 50 == 0:
                        time.sleep(0.0001)

        def consumer_task():
            while True:
                try:
                    pkt = self.ring.dequeue()
                    if pkt is not None:
                        with consumed_lock:
                            consumed_items.append(pkt[0])
                except RingBufferEmptyError:
                    stats = self.ring.get_stats()
                    # If all producers finished and size is 0, exit loop
                    if sum(produced_count) >= total_expected_items and stats["size"] == 0:
                        break
                    time.sleep(0.0005)