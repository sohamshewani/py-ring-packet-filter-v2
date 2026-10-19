# -*- coding: utf-8 -*-
"""
py_ring.arena - High-Performance Contiguous Memory Arena and Slot Manager
System: py-ring-packet-filter
"""

import threading
from typing import Optional, List, Tuple

class ArenaError(Exception):
    """Base exception for arena allocation and lifecycle failures."""
    pass


class ArenaExhaustedError(ArenaError):
    """Raised when the arena has no available free slots."""
    pass


class InvalidSlotError(ArenaError):
    """Raised when an operation is attempted on an invalid or unowned slot index."""
    pass


class SlotState:
    """Bitmask constants tracking the lifecycle ownership of individual arena slots."""
    FREE: int = 0x01
    ALLOCATED: int = 0x02
    PROCESSING: int = 0x04
    DIRTY: int = 0x08


class ArenaSlot:
    """
    Represents a metadata handle to a specific slice within the backing bytearray.
    Maintains zero-copy memoryviews and strict ownership state flags.
    """
    __slots__ = ('_index', '_buffer', '_view', '_state', '_length')

    def __init__(self, index: int, backing_buffer: bytearray, offset: int, slot_size: int) -> None:
        self._index: int = index
        self._buffer: bytearray = backing_buffer
        self._view: memoryview = memoryview(backing_buffer)[offset : offset + slot_size]
        self._state: int = SlotState.FREE
        self._length: int = slot_size

    @property
    def index(self) -> int:
        return self._index

    @property
    def view(self) -> memoryview:
        return self._view

    @property
    def state(self) -> int:
        return self._state

    @property
    def length(self) -> int:
        return self._length

    def mark_allocated(self) -> None:
        self._state = SlotState.ALLOCATED

    def mark_processing(self) -> None:
        self._state = SlotState.PROCESSING

    def mark_dirty(self) -> None:
        self._state = SlotState.DIRTY

    def mark_free(self) -> None:
        self._state = SlotState.FREE

    def clear(self) -> None:
        # Zero out the physical bytes in the backing buffer view
        self._view[:] = b'\x00' * self._length
        self._state = SlotState.FREE


class MemoryArena:
    """
    Contiguous pre-allocated Arena memory block divided into fixed-size slot allocations.
    Eliminates garbage collection pressure via pointer-free slot offsets backed by bytearrays
    and memoryviews. Includes thread-safe acquisition and release monitors.
    """

    def __init__(self, capacity: int, slot_size: int) -> None:
        if capacity <= 0 or (capacity & (capacity - 1)) != 0:
            raise ValueError("Arena capacity must be a positive power of two.")
        if slot_size <= 0:
            raise ValueError("Arena slot_size must be greater than zero.")

        self._capacity: int = capacity
        self._slot_size: int = slot_size
        self._total_size: int = capacity * slot_size

        # Contiguous pre-allocated memory block
        self._memory: bytearray = bytearray(self._total_size)
        
        # Initialize slots
        self._slots: List[ArenaSlot] = [
            ArenaSlot(
                index=i,
                backing_buffer=self._memory,
                offset=i * slot_size,
                slot_size=slot_size
            )
            for i in range(capacity)
        ]

        # Free list tracking slot indices using a pre-allocated stack for O(1) ops
        self._free_stack: List[int] = list(range(capacity - 1, -1, -1))
        
        # Concurrency monitor
        self._lock: threading.Lock = threading.Lock()
        
        # Metrics
        self._active_allocations: int = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def slot_size(self) -> int:
        return self._slot_size

    @property
    def total_size(self) -> int:
        return self._total_size

    @property
    def active_allocations(self) -> int:
        with self._lock:
            return self._active_allocations

    def allocate(self) -> ArenaSlot:
        """
        Allocates a free arena slot in O(1) time.
        Raises ArenaExhaustedError if all slots are currently owned by the pipeline.
        """
        with self._lock:
            if not self._free_stack:
                raise ArenaExhaustedError(
                    f"MemoryArena exhausted: capacity {self._capacity} slots fully saturated."
                )
            
            slot_idx = self._free_stack.pop()
            slot = self._slots[slot_idx]
            
            if slot.state != SlotState.FREE:
                # Fatal invariant check
                self._free_stack.append(slot_idx)
                raise ArenaError(f"Critical corruption: Free stack returned non-free slot {slot_idx}.")
            
            slot.mark_allocated()
            self._active_allocations += 1
            return slot

    def release(self, slot: ArenaSlot) -> None:
        """
        Returns a slot to the arena free stack, zeroing its contents to prevent data leaks.
        """
        if not isinstance(slot, ArenaSlot):
            raise InvalidSlotError("Provided object is not an instance of ArenaSlot.")

        idx = slot.index
        if idx < 0 or idx >= self._capacity:
            raise InvalidSlotError(f"Slot index {idx} out of arena bounds [0, {self._capacity}).")

        with self._lock:
            if slot.state == SlotState.FREE:
                raise InvalidSlotError(f"Double-free detected on arena slot {idx}.")

            slot.clear()
            self._free_stack.append(idx)
            self._active_allocations -= 1

    def get_slot(self, index: int) -> ArenaSlot:
        """
        Directly retrieves a slot by its absolute index without changing its state.
        Used primarily by ring buffer indexing.
        """
        if index < 0 or index >= self._capacity:
            raise IndexError(f"Slot index {index} out of range for arena capacity {self._capacity}.")
        return self._slots[index]

    def reset(self) -> None:
        """
        Forcefully resets the entire arena, reclaiming all slots and zeroing memory.
        Thread-safe administrative utility.
        """
        with self._lock:
            self._memory[:] = b'\x00' * self._total_size
            self._free_stack = list(range(self._capacity - 1, -1, -1))
            for slot in self._slots:
                slot._state = SlotState.FREE
            self._active_allocations = 0

    def validate_integrity(self) -> Tuple[bool, str]:
        """
        Performs structural integrity checks on the arena metadata and free stack.
        Returns a tuple of (is_valid, diagnostic_message).
        """
        with self._lock:
            free_set = set(self._free_stack)
            if len(free_set) != len(self._free_stack):
                return False, "Duplicate slot entries found in free stack."

            allocated_count = 0
            for i, slot in enumerate(self._slots):
                if slot.index != i:
                    return False, f"Slot index mismatch at position {i}."
                if slot.state == SlotState.FREE:
                    if i not in free_set:
                        return False, f"Free slot {i} is missing from the free stack."
                else:
                    if i in free_set:
                        return False, f"Active slot {i} is incorrectly present in the free stack."
                    allocated_count += 1

            if allocated_count != self._active_allocations:
                return False, f"Allocation count mismatch: tracked {self._active_allocations}, computed {allocated_count}."

            return True, "Arena integrity verified successfully."