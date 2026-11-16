"""Regression guards for MLflow experiment wiring (no Spark).

Background: a first `databricks bundle deploy` to a fresh workspace failed with

    cannot create mlflow experiment: Parent directory does not exist:
    /Users/<user>/dsa_interview_packet (404 NOT_FOUND)

because the declarative ``experiments`` resource named experiments inside a
``dsa_interview_packet/`` folder that the bundle never created. MLflow experiments
are workspace objects (not Unity Catalog securables), and the experiments/create
API does not create intermediate directories. The fix names every experiment
directly under the user's home directory, whose parent always exists.

A second, subtler bug: in ``mode: development`` the bundle prefixes the declared
experiment name with ``[dev <user>]``, so a script that hand-builds the name would
log runs to a *different* experiment than the tagged, bundle-managed one. The fix
passes each managed experiment's resolved name into the scripts via a job
parameter (``${resources.experiments.*.name}``). These tests fail if anyone
re-nests a path (bringing back the 404), drops the job-parameter wiring (bringing
back the split-experiment bug), or lets the declared names drift from the scripts.
"""

# mypy: ignore-errors

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_ML_MODELS_YML = _ROOT / "resources" / "ml_models.yml"
_JOBS_YML = _ROOT / "resources" / "jobs.yml"
_SCRIPTS_DIR = _ROOT / "src" / "dsa_interview_packet" / "scripts"
_SCRIPT_FILES = (
    "train_demand_forecast.py",
    "train_supplier_scoring.py",
    "evaluate_models.py",
)

# A ``name:`` line inside ml_models.yml declaring an experiment path. Parsed
# textually (rather than via a YAML lib) to keep this guard dependency-free.
_YAML_NAME_LINE_RE = re.compile(
    r"^\s*name:\s*(?P<name>/Workspace/Users/\S+)\s*$", re.MULTILINE
)

# A declared experiment resource key, e.g. "    demand_forecast_experiment:".
_YAML_EXPERIMENT_KEY_RE = re.compile(r"^    (?P<key>\w+_experiment):\s*$", re.MULTILINE)

# Declarative experiment names look like:
#   /Workspace/Users/${workspace.current_user.userName}/<leaf>
# The leaf must contain no "/" so the parent (the user home) always exists.
_YAML_NAME_RE = re.compile(
    r"^/Workspace/Users/\$\{workspace\.current_user\.userName\}/(?P<leaf>[^/]+)$"
)

# Script fallback names are f-strings: f"/Users/{<var>}/<leaf>". Match any loop
# variable name (not a hard-coded one) so a benign rename does not break the guard.
_SCRIPT_NAME_RE = re.compile(r'/Users/\{[^}]+\}/(?P<leaf>[^"]+)"')

# A bundle reference to a managed experiment's resolved name.
_JOB_EXPERIMENT_REF_RE = re.compile(
    r"\$\{resources\.experiments\.(?P<key>\w+)\.name\}"
)


def _yaml_experiment_names() -> list[str]:
    return _YAML_NAME_LINE_RE.findall(_ML_MODELS_YML.read_text())


def _yaml_experiment_keys() -> list[str]:
    return _YAML_EXPERIMENT_KEY_RE.findall(_ML_MODELS_YML.read_text())


def _script_experiment_leaves() -> dict[str, list[str]]:
    """Map each script file -> list of fallback experiment leaf names it references."""
    out: dict[str, list[str]] = {}
    for fname in _SCRIPT_FILES:
        text = (_SCRIPTS_DIR / fname).read_text()
        out[fname] = _SCRIPT_NAME_RE.findall(text)
    return out


def test_yaml_experiments_are_flat_under_user_home() -> None:
    """Declared experiment paths must live directly under the user home (no 404)."""
    names = _yaml_experiment_names()
    assert names, "expected at least one experiment declared in ml_models.yml"
    for name in names:
        m = _YAML_NAME_RE.match(name)
        assert m is not None, (
            f"experiment name {name!r} is not flat under the user home. Name it "
            f"'/Workspace/Users/${{workspace.current_user.userName}}/<leaf>' with no "
            f"extra '/': a nested folder is not created at deploy time and returns "
            f"404 'Parent directory does not exist'."
        )
        assert "/" not in m.group("leaf")


def test_script_experiment_paths_are_flat_under_user_home() -> None:
    """Fallback set_experiment() paths in the scripts must also be flat."""
    for fname, leaves in _script_experiment_leaves().items():
        assert leaves, f"{fname}: expected at least one /Users/<var>/... fallback path"
        for leaf in leaves:
            assert "/" not in leaf, (
                f"{fname}: experiment path '/Users/<user>/{leaf}' is nested. "
                f"Keep experiments directly under the user home so the parent exists."
            )


def test_yaml_names_match_script_paths() -> None:
    """Declared names must equal the fallback leaves the scripts use (no drift)."""
    yaml_leaves = {
        _YAML_NAME_RE.match(name).group("leaf") for name in _yaml_experiment_names()
    }
    all_script_leaves = {
        leaf for leaves in _script_experiment_leaves().values() for leaf in leaves
    }
    missing = yaml_leaves - all_script_leaves
    assert not missing, (
        f"experiments declared in ml_models.yml but never used by a training/eval "
        f"script (names drifted): {sorted(missing)}"
    )


def test_every_declared_experiment_is_injected_into_a_job() -> None:
    """Each managed experiment must be wired into a job via ${resources...name}.

    This is what guarantees runs land in the tagged, bundle-managed experiment
    (including the [dev <user>] prefix) rather than a hand-built look-alike. If the
    wiring is dropped, the split-experiment bug returns silently.
    """
    declared = set(_yaml_experiment_keys())
    assert declared, "expected experiment resources declared in ml_models.yml"
    referenced = set(_JOB_EXPERIMENT_REF_RE.findall(_JOBS_YML.read_text()))
    missing = declared - referenced
    assert not missing, (
        f"experiment(s) declared in ml_models.yml but never injected into a job "
        f"via ${{resources.experiments.<key>.name}} in jobs.yml: {sorted(missing)}. "
        f"Without the job parameter, runs log to a different (untagged) experiment "
        f"in development mode."
    )


def test_scripts_consume_the_injected_experiment_name() -> None:
    """The scripts must accept + use the injected experiment name, not only the fallback."""
    train_demand = (_SCRIPTS_DIR / "train_demand_forecast.py").read_text()
    train_supplier = (_SCRIPTS_DIR / "train_supplier_scoring.py").read_text()
    evaluate = (_SCRIPTS_DIR / "evaluate_models.py").read_text()

    for name, text in (("train_demand", train_demand), ("train_supplier", train_supplier)):
        assert "--experiment-name" in text, f"{name}: missing --experiment-name arg"
        assert "experiment_name=args.experiment_name" in text, (
            f"{name}: does not pass the injected experiment name into main()"
        )

    for flag in (
        "--demand-experiment-name",
        "--scoring-experiment-name",
        "--evaluation-experiment-name",
    ):
        assert flag in evaluate, f"evaluate_models.py: missing {flag} arg"
