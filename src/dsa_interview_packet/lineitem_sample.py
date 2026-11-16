"""Optional downsampling of ``lineitem`` for faster ML training and evaluation."""

from __future__ import annotations

from typing import Any


def sample_lineitem(lineitem: Any, fraction: float, *, seed: int = 42) -> Any:
    """Return a random subset of lineitem rows (fixed seed for reproducibility).

    Use ``fraction=1.0`` for the full table. Values in ``(0, 1)`` use
    :meth:`DataFrame.sample` before joins so window features run on less data.
    """
    if fraction > 1.0:
        msg = "lineitem sample fraction must be <= 1.0"
        raise ValueError(msg)
    if fraction <= 0.0:
        msg = "lineitem sample fraction must be > 0.0"
        raise ValueError(msg)
    if fraction >= 1.0:
        return lineitem
    return lineitem.sample(False, fraction, seed)
