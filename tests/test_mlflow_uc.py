"""Unit tests for Unity Catalog model-registration helpers (no Spark).

These guard the decision + naming logic used by the training scripts so a model
can never be registered under a malformed name by mistake.
"""

# mypy: ignore-errors

from __future__ import annotations

import pytest

from dsa_interview_packet.mlflow_uc import (
    UC_REGISTRY_URI,
    resolve_registered_model_name,
    uc_model_name,
)


def test_registry_uri_targets_unity_catalog() -> None:
    assert UC_REGISTRY_URI == "databricks-uc"


def test_uc_model_name_builds_three_level_name() -> None:
    assert (
        uc_model_name("workspace", "dsa_interview_supply_chain", "demand_forecast")
        == "workspace.dsa_interview_supply_chain.demand_forecast"
    )


@pytest.mark.parametrize(
    ("catalog", "schema", "model"),
    [
        ("bad-catalog", "s", "m"),  # hyphen not allowed
        ("c", "bad schema", "m"),  # space not allowed
        ("c", "s", "1model"),  # cannot start with a digit
        ("c", "s", ""),  # empty segment
        ("c", "s", "a.b"),  # embedded dot would create a 4-level name
    ],
)
def test_uc_model_name_rejects_invalid_identifiers(
    catalog: str, schema: str, model: str
) -> None:
    with pytest.raises(ValueError):
        uc_model_name(catalog, schema, model)


# --- resolve_registered_model_name: the per-run decision the scripts pass to
#     mlflow.spark.log_model(registered_model_name=...). ------------------------


def test_resolve_registers_when_enabled() -> None:
    """Registration enabled -> the three-level model name."""
    assert (
        resolve_registered_model_name(
            "workspace",
            "dsa_interview_supply_chain",
            "demand_forecast",
            register_to_uc=True,
        )
        == "workspace.dsa_interview_supply_chain.demand_forecast"
    )


def test_resolve_returns_none_when_registration_disabled() -> None:
    """--no-uc-model-registry -> artifact-only logging."""
    assert (
        resolve_registered_model_name(
            "workspace", "sch", "supplier_scoring", register_to_uc=False
        )
        is None
    )


def test_resolve_rejects_malformed_name_when_registering() -> None:
    """A malformed identifier must raise (not silently skip) when we would register."""
    with pytest.raises(ValueError):
        resolve_registered_model_name(
            "workspace", "bad schema", "demand_forecast", register_to_uc=True
        )


@pytest.mark.parametrize("model", ["demand_forecast", "supplier_scoring"])
def test_resolve_covers_both_model_names_used_by_scripts(model: str) -> None:
    """Guard the exact model leaf names the two training scripts register."""
    name = resolve_registered_model_name(
        "workspace", "dsa_interview_supply_chain", model, register_to_uc=True
    )
    assert name == f"workspace.dsa_interview_supply_chain.{model}"
