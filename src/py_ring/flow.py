# -*- coding: utf-8 -*-
"""
py_ring.flow
~~~~~~~~~~~~

High-performance, zero-copy packet flow tracking, sliding-window management,
and flow-control state machines for the py-ring-packet-filter system.
"""

from __future__ import annotations

import sys
import time
from typing import Optional, Final, Tuple

# Ensure strict runtime layout and typing performance
__all__ = [
    "FlowState",
    "SlidingWindowTracker",
    "FlowContext",
    "FlowManager",
]


class FlowState:
    """Enumeration of flow lifecycle states."""
    IDLE: Final[int] = 0
    ESTABLISHED: Final[int] = 1
    CONGESTED: Final[int] = 2
    TEARDOWN: Final[int] = 3
    CLOSED: Final[int] = 4


class SlidingWindowTracker:
    """
    Sliding-window tracking structure maintaining delivery bitmaps and
    rate-limit states for a distinct packet flow. Operates with zero dynamic
    allocation during steady-state packet processing.
    """

    __slots__ = (
        "_window_size",
        "_left_edge",
        "_bitmap",
        "_rate_limit_bps",
        "_tokens",
        "_last_replenish_ns",
        "_packet_count",
        "_byte_count",
    )

    def __init__(self, window_size: int = 64, rate_limit_bps: int = 0) -> None:
        if window_size <= 0 or (window_size & (window_size - 1)) != 0:
            # Force power of two or standard sizing, here we enforce multiples of 64 for bitmap word alignment
            window_size = ((window_size + 63) // 64) * 64

        self._window_size: Final[int] = window_size
        self._left_edge: int = 0
        self._bitmap: int = 0  # Bitmask representing received sequence numbers relative to left_edge
        
        self._rate_limit_bps: int = rate_limit_bps
        self._tokens: float = float(rate_limit_bps) if rate_limit_bps > 0 else 0.0
        self._last_replenish_ns: int = time.monotonic_ns()
        
        self._packet_count: int = 0
        self._byte_count: int = 0

    @property
    def left_edge(self) -> int:
        return self._left_edge

    @property
    def window_size(self) -> int:
        return self._window_size

    def check_and_update(self, seq: int, packet_len: int) -> Tuple[bool, bool]:
        """
        Validates a sequence number against the sliding horizon and updates
        delivery bitmaps.

        Returns:
            (is_valid, is_duplicate)
        """
        # Out-of-order packet exceeding the sliding horizon (too far ahead)
        if seq >= self._left_edge + self._window_size:
            return False, False

        # Packet is older than the current sliding window (already acknowledged/dropped)
        if seq < self._left_edge:
            return False, True

        # Calculate bit offset relative to left edge
        offset = seq - self._left_edge
        mask = 1 << offset

        # Check if already received (duplicate)
        if (self._bitmap & mask) != 0:
            return True, True

        # Mark as received
        self._bitmap |= mask
        self._packet_count += 1
        self._byte_count += packet_len

        # Advance window left edge if contiguous blocks are filled
        if offset == 0:
            self._advance_window()

        return True, False

    def _advance_window(self) -> None:
        """Slides the window forward by clearing contiguous lower bits."""
        shift = 0
        # Find how many contiguous set bits exist from the lowest bit
        # Using bitwise operations to count trailing ones efficiently
        temp = self._bitmap
        while (temp & 1) != 0:
            shift += 1
            temp >>= 1

        if shift > 0:
            self._left_edge += shift
            self._bitmap >>= shift

    def check_rate_limit(self, packet_len: int) -> bool:
        """
        Token-bucket rate limiting check. Returns True if packet is permitted,
        False if rate limit is exceeded.
        """
        if self._rate_limit_bps <= 0:
            return True

        now = time.monotonic_ns()
        elapsed_ns = now - self._last_replenish_ns
        self._last_replenish_ns = now

        # Replenish tokens based on elapsed time (tokens in bytes per second)
        # 1 second = 1,000,000,000 ns
        refill = (elapsed_ns * self._rate_limit_bps) / 8_000_000_000.stat if hasattr(self, '_') else (elapsed_ns * self._rate_limit_bps) / 8_000_000_000.0
        max_tokens = float(self._rate_limit_bps // 8)  # 1 second burst capacity in bytes
        
        self._tokens = min(max_tokens, self._tokens + refill)

        packet_bytes = float(packet_len)
        if self._tokens >= packet_bytes:
            self._tokens -= packet_bytes
            return True

        return False

    def reset(self) -> None:
        self._left_edge = 0
        self._bitmap = 0
        self._tokens = float(self._rate_limit_bps) if self._rate_limit_bps > 0 else 0.0
        self._last_replenish_ns = time.monotonic_ns()
        self._packet_count = 0
        self._byte_count = 0


class FlowContext:
    """
    Maintains complete state for an active network flow, integrating sliding
    window tracking, state machine transitions, and activity metrics.
    """

    __slots__ = (
        "_flow_id",
        "_state",
        "_window_tracker",
        "_created_at_ns",
        "_last_active_ns",
        "_priority",
    )

    def __init__(self, flow_id: int, window_size: int = 64, rate_limit_bps: int = 0, priority: int = 0) -> None:
        self._flow_id: Final[int] = flow_id
        self._state: int = FlowState.ESTABLISHED
        self._window_tracker: Final[SlidingWindowTracker] = SlidingWindowTracker(
            window_size=window_size, rate_limit_bps=rate_limit_bps
        )
        self._created_at_ns: Final[int] = time.monotonic_ns()
        self._last_active_ns: int = self._created_at_ns
        self._priority: int = priority

    @property
    def flow_id(self) -> int:
        return self._flow_id

    @property
    def state(self) -> int:
        return self._state

    @state.setter
    def state(self, new_state: int) -> None:
        self._state = new_state

    @property
    def tracker(self) -> SlidingWindowTracker:
        return self._window_tracker

    @property
    def last_active_ns(self) -> int:
        return self._last_active_ns

    def touch(self) -> None:
        self._last_active_ns = time.monotonic_ns()

    def process_packet(self, seq: int, packet_len: int) -> Tuple[bool, bool, bool]:
        """
        Processes a packet against the flow context.

        Returns:
            (is_accepted, is_duplicate, rate_limited)
        """
        if self._state == FlowState.CLOSED or self._state == FlowState.TEARDOWN:
            return False, False, False

        self.touch()

        # Check rate limit
        if not self._window_tracker.check_rate_limit(packet_len):
            return False, False, True

        # Check sliding window
        valid, duplicate = self._window_tracker.check_and_update(seq, packet_len)
        if not valid:
            return False, duplicate, False

        return True, duplicate, False


class FlowManager:
    """
    Manages a collection of active flow contexts with fixed memory overhead,
    supporting fast lookup, idle flow eviction, and global flow metrics.
    """

    __slots__ = (
        "_max_flows",
        "_flows",
        "_idle_timeout_ns",
    )

    def __init__(self, max_flows: int = 1024, idle_timeout_s: float = 30.0) -> None:
        if max_flows <= 0:
            raise ValueError("max_flows must be greater than zero.")
        
        self._max_flows: Final[int] = max_flows
        self._flows: dict[int, FlowContext] = {}
        self._idle_timeout_ns: Final[int] = int(idle_timeout_s * 1_000_000_000)

    @property
    def active_flow_count(self) -> int:
        return len(self._flows)

    def get_or_create_flow(self, flow_id: int, window_size: int = 64, rate_limit_bps: int = 0) -> FlowContext:
        """Retrieves an existing flow or allocates a new one, evicting idle flows if capacity is reached."""
        flow = self._flows.get(flow_id)
        if flow is not None:
            flow.touch()
            return flow

        # Enforce capacity limits via eviction if necessary
        if len(self._flows) >= self._max_flows:
            self._evict_oldest_or_idle_flow()

        flow = FlowContext(flow_id=flow_id, window_size=window_size, rate_limit_bps=rate_limit_bps)
        self._flows[flow_id] = flow
        return flow

    def get_flow(self, flow_id: int) -> Optional[FlowContext]:
        flow = self._flows.get(flow_id)
        if flow is not None:
            flow.touch()
        return flow

    def terminate_flow(self, flow_id: int) -> bool:
        flow = self._flows.pop(flow_id, None)
        if flow is not None:
            flow.state = FlowState.CLOSED
            return True
        return False

    def reap_idle_flows(self) -> int:
        """Scans and removes flows that have exceeded the idle timeout threshold."""
        now = time.monotonic_ns()
        expired_ids = [
            fid for fid, flow in self._flows.items()
            if (now - flow.last_active_ns) > self._idle_timeout_ns
        ]
        for fid in expired_ids:
            flow = self._flows.pop(fid, None)
            if flow is not None:
                flow.state = FlowState.CLOSED
        return len(expired_ids)

    def _evict_oldest_or_idle_flow(self) -> None:
        """Evicts the flow with the oldest last_active_ns timestamp to free capacity."""
        if not self._flows:
            return

        oldest_fid = min(self._flows.keys(), key=lambda fid: self._flows[fid].last_active_ns)
        evicted = self._flows.pop(oldest_fid, None)
        if evicted is not None:
            evicted.state = FlowState.CLOSED