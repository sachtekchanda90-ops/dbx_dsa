"""Prepend the bundle ``src`` directory to ``sys.path`` for job execution.

Serverless ``spark_python_task`` runs a single file; Python does not
automatically add the synced repo ``src`` tree to the import path, so
``import dsa_interview_packet`` fails unless we add ``.../src`` here.
"""

from __future__ import annotations

import os
import sys


def ensure() -> None:
    """Insert the bundle ``src`` root at the front of ``sys.path`` if missing."""
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    src_root = os.path.dirname(os.path.dirname(scripts_dir))
    if src_root not in sys.path:
        sys.path.insert(0, src_root)


ensure()
