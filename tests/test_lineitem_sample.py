"""Tests for lineitem downsampling helper."""


# mypy: ignore-errors

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from dsa_interview_packet.lineitem_sample import sample_lineitem


def test_sample_lineitem_full_returns_same_dataframe() -> None:
    mock_df = MagicMock()
    out = sample_lineitem(mock_df, 1.0)
    assert out is mock_df
    mock_df.sample.assert_not_called()


def test_sample_lineitem_rejects_invalid_fraction() -> None:
    mock_df = MagicMock()
    with pytest.raises(ValueError, match="> 0"):
        sample_lineitem(mock_df, 0.0)
    with pytest.raises(ValueError, match="<="):
        sample_lineitem(mock_df, 1.01)
