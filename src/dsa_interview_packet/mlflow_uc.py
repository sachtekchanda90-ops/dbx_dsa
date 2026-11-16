"""Register trained models in the Unity Catalog Model Registry.

MLflow **experiments** are workspace objects and cannot live in Unity Catalog
(see ``resources/ml_models.yml``). Trained **models**, however, can be registered
as first-class UC securables named ``catalog.schema.model`` via the
``databricks-uc`` registry URI -- giving them UC grants, lineage, and
cross-workspace discovery.

This module holds the pure decision + naming logic so it is unit-testable without
Spark or a live workspace. Free Edition is Unity-Catalog-only, so registration is
always available; callers may still opt out (``register_to_uc=False``) to log the
model as a run artifact only.
"""

from __future__ import annotations

import re

# MLflow registry URI that targets the Unity Catalog Model Registry.
UC_REGISTRY_URI = "databricks-uc"

_SQL_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_sql_ident(segment: str, *, kind: str) -> str:
    if not _SQL_IDENT_RE.fullmatch(segment):
        msg = f"Invalid {kind} identifier for UC model name: {segment!r}"
        raise ValueError(msg)
    return segment


def uc_model_name(catalog: str, schema: str, model: str) -> str:
    """Return the validated three-level UC model name ``catalog.schema.model``.

    Raises ``ValueError`` if any segment is not a plain SQL identifier, so a
    malformed name can never be silently registered to the wrong place.
    """
    return ".".join(
        (
            _validate_sql_ident(catalog, kind="catalog"),
            _validate_sql_ident(schema, kind="schema"),
            _validate_sql_ident(model, kind="model"),
        )
    )


def resolve_registered_model_name(
    catalog: str,
    schema: str,
    model: str,
    *,
    register_to_uc: bool,
) -> str | None:
    """Decide the UC registered-model name for a training run.

    Returns the validated ``catalog.schema.model`` name when ``register_to_uc`` is
    set, or ``None`` when the caller should log the model as a run artifact only
    (registration explicitly disabled). This is the single decision the training
    scripts pass to ``mlflow.spark.log_model(registered_model_name=...)``.
    """
    if not register_to_uc:
        return None
    return uc_model_name(catalog, schema, model)
