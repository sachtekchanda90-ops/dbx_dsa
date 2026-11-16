"""Tests for the packet Makefile's workspace targets (no Spark, no Databricks).

Each test copies the real Makefile into a temp directory and runs ``make`` with a
fake ``databricks`` CLI first on PATH. The fake records every call and never
contacts a workspace, so these tests are safe to run anywhere ``make`` exists.

They pin the behavior the setup docs rely on: job targets deploy your current
files before running, ``VARS`` is saved by a successful deploy and reused, and
sign-in failures print a clear next step.
"""


# mypy: ignore-errors

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

PACKET_DIR = Path(__file__).resolve().parent.parent
MAKE = shutil.which("make")

pytestmark = pytest.mark.skipif(MAKE is None, reason="make is not installed")

FAKE_CLI = """#!/bin/sh
echo "$*" >> "$FAKE_LOG"
case "$1 $2" in
  "current-user me") [ -z "$FAKE_AUTH_FAIL" ] ;;
  "auth profiles") printf 'Name  Host  Valid\\nalpha  https://a.example  NO\\nbeta  https://b.example  NO\\n' ;;
  "bundle deploy") [ -z "$FAKE_DEPLOY_FAIL" ] ;;
  "bundle destroy") [ -z "$FAKE_DESTROY_FAIL" ] ;;
esac
"""

# Makefile variables a developer may have exported; scrub them so tests start clean.
_SCRUBBED_ENV = ("VARS", "PROFILE", "TARGET", "AUTO_APPROVE", "MAKEFLAGS", "MFLAGS", "MAKELEVEL")


class Packet:
    """A temp copy of the Makefile plus a fake CLI that logs its arguments."""

    def __init__(self, tmp_path: Path):
        self.dir = tmp_path / "packet"
        self.dir.mkdir()
        shutil.copy(PACKET_DIR / "Makefile", self.dir / "Makefile")
        self.bin = tmp_path / "bin"
        self.bin.mkdir()
        cli = self.bin / "databricks"
        cli.write_text(FAKE_CLI)
        cli.chmod(0o755)
        self.log = tmp_path / "cli.log"
        self.vars_file = self.dir / ".bundle-vars-dev"

    def make(self, *args: str, path: str | None = None, **env: str) -> subprocess.CompletedProcess:
        run_env = {k: v for k, v in os.environ.items() if k not in _SCRUBBED_ENV}
        run_env["PATH"] = path if path is not None else f"{self.bin}{os.pathsep}{os.environ['PATH']}"
        run_env["FAKE_LOG"] = str(self.log)
        run_env.update(env)
        return subprocess.run(
            [MAKE, "--no-print-directory", *args],
            cwd=self.dir,
            env=run_env,
            capture_output=True,
            text=True,
            check=False,
        )

    def calls(self) -> list[str]:
        """CLI calls in order, without the pre-flight sign-in checks."""
        if not self.log.exists():
            return []
        lines = self.log.read_text().splitlines()
        return [c for c in lines if not c.startswith(("current-user", "auth profiles"))]

    def all_calls(self) -> list[str]:
        return self.log.read_text().splitlines() if self.log.exists() else []


@pytest.fixture
def packet(tmp_path):
    return Packet(tmp_path)


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------

def test_help_runs(packet):
    result = packet.make("help")
    assert result.returncode == 0, result.stderr
    for target in ("setup", "test", "deploy", "seed", "simulate", "ml", "init", "integration", "destroy"):
        assert re.search(rf"^  {target} ", result.stdout, re.M), f"`make help` no longer lists {target}"


def test_every_job_the_makefile_runs_exists_in_the_bundle():
    makefile = (PACKET_DIR / "Makefile").read_text()
    run_jobs = set(re.findall(r"^\t\$\(RUN\) (\w+)", makefile, re.M))
    jobs_yml = (PACKET_DIR / "resources" / "jobs.yml").read_text()
    bundle_jobs = set(re.findall(r"^    (\w+):$", jobs_yml, re.M))
    assert run_jobs, "found no `$(RUN) <job>` lines in the Makefile"
    missing = run_jobs - bundle_jobs
    assert not missing, f"Makefile runs jobs not defined in resources/jobs.yml: {sorted(missing)}"


def test_workspace_targets_are_phony():
    makefile = (PACKET_DIR / "Makefile").read_text()
    phony_block = re.search(r"^\.PHONY:(.*?)(?<!\\)\n", makefile, re.M | re.S).group(1)
    declared = set(phony_block.replace("\\", " ").split())
    expected = {"validate", "deploy", "seed", "simulate", "ml", "init", "integration", "destroy",
                "_require-venv", "_require-workspace"}
    assert not expected - declared, f"missing from .PHONY: {sorted(expected - declared)}"


# ---------------------------------------------------------------------------
# Job targets deploy first
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("target", "jobs"),
    [
        ("seed", ["seed_source_tables"]),
        ("simulate", ["simulate_system_tables"]),
        ("ml", ["ml_train_eval_pipeline"]),
        ("integration", ["run_integration_tests"]),
        ("init", ["seed_source_tables", "simulate_system_tables", "ml_train_eval_pipeline"]),
    ],
)
def test_job_targets_deploy_then_run(packet, target, jobs):
    result = packet.make(target)
    assert result.returncode == 0, result.stdout + result.stderr
    assert packet.calls() == ["bundle deploy -t dev"] + [f"bundle run -t dev {job}" for job in jobs]


def test_several_job_targets_deploy_once(packet):
    result = packet.make("seed", "simulate", "ml")
    assert result.returncode == 0, result.stdout + result.stderr
    assert [c for c in packet.calls() if c.startswith("bundle deploy")] == ["bundle deploy -t dev"]


def test_failed_deploy_does_not_run_the_job(packet):
    result = packet.make("seed", FAKE_DEPLOY_FAIL="1")
    assert result.returncode != 0
    assert not any(c.startswith("bundle run") for c in packet.calls())


def test_profile_and_target_are_passed_through(packet):
    result = packet.make("integration", "PROFILE=myprof", "TARGET=dev_fast")
    assert result.returncode == 0, result.stdout + result.stderr
    assert packet.calls() == [
        "bundle deploy -t dev_fast --profile myprof",
        "bundle run -t dev_fast --profile myprof run_integration_tests",
    ]


# ---------------------------------------------------------------------------
# VARS is saved by a successful deploy and reused
# ---------------------------------------------------------------------------

OVERRIDE = "--var=catalog=my_catalog --var=schema=my_schema"


def test_deploy_saves_vars_and_later_commands_reuse_them(packet):
    assert packet.make("deploy", f"VARS={OVERRIDE}").returncode == 0
    assert packet.vars_file.read_text().strip() == OVERRIDE

    result = packet.make("simulate")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Using saved VARS" in result.stdout
    assert packet.calls()[-2:] == [
        f"bundle deploy -t dev {OVERRIDE}",
        f"bundle run -t dev {OVERRIDE} simulate_system_tables",
    ]


def test_validate_reuses_saved_vars(packet):
    packet.vars_file.write_text(OVERRIDE + "\n")
    assert packet.make("validate").returncode == 0
    assert packet.calls() == [f"bundle validate -t dev {OVERRIDE}"]


def test_new_vars_replace_saved_ones(packet):
    packet.vars_file.write_text(OVERRIDE + "\n")
    assert packet.make("deploy", "VARS=--var=schema=other").returncode == 0
    assert packet.vars_file.read_text().strip() == "--var=schema=other"


def test_empty_vars_resets_to_defaults(packet):
    packet.vars_file.write_text(OVERRIDE + "\n")
    result = packet.make("deploy", "VARS=")
    assert result.returncode == 0, result.stdout + result.stderr
    assert packet.calls() == ["bundle deploy -t dev"]
    assert not packet.vars_file.exists()


def test_failed_deploy_does_not_save_vars(packet):
    result = packet.make("deploy", f"VARS={OVERRIDE}", FAKE_DEPLOY_FAIL="1")
    assert result.returncode != 0
    assert not packet.vars_file.exists()


def test_no_saved_vars_uses_defaults_quietly(packet):
    result = packet.make("deploy")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Using saved VARS" not in result.stdout
    assert packet.calls() == ["bundle deploy -t dev"]
    assert not packet.vars_file.exists()


def test_destroy_uses_saved_vars_then_removes_them(packet):
    packet.vars_file.write_text(OVERRIDE + "\n")
    result = packet.make("destroy")
    assert result.returncode == 0, result.stdout + result.stderr
    assert packet.calls() == [f"bundle destroy -t dev {OVERRIDE}"]
    assert not packet.vars_file.exists()


def test_destroy_auto_approve_skips_the_prompt(packet):
    assert packet.make("destroy", "AUTO_APPROVE=1").returncode == 0
    assert packet.calls() == ["bundle destroy -t dev --auto-approve"]


def test_auto_approve_needs_exactly_1(packet):
    for value in ("0", "no", "true"):
        assert packet.make("destroy", f"AUTO_APPROVE={value}").returncode == 0
    assert packet.calls() == ["bundle destroy -t dev"] * 3


def test_failed_auto_approved_destroy_skips_the_prompt_hint(packet):
    result = packet.make("destroy", "AUTO_APPROVE=1", FAKE_DESTROY_FAIL="1")
    assert result.returncode != 0
    assert "asks for confirmation" not in result.stdout


def test_failed_destroy_explains_and_keeps_saved_vars(packet):
    packet.vars_file.write_text(OVERRIDE + "\n")
    result = packet.make("destroy", FAKE_DESTROY_FAIL="1")
    assert result.returncode != 0
    assert "make destroy AUTO_APPROVE=1" in result.stdout
    assert packet.vars_file.exists()


def test_saved_vars_are_per_target(packet):
    assert packet.make("deploy", f"VARS={OVERRIDE}").returncode == 0
    assert packet.make("deploy", "TARGET=dev_fast", "VARS=--var=schema=fast").returncode == 0
    assert packet.vars_file.read_text().strip() == OVERRIDE
    assert (packet.dir / ".bundle-vars-dev_fast").read_text().strip() == "--var=schema=fast"


def test_destroying_one_target_keeps_the_other_targets_vars(packet):
    packet.vars_file.write_text(OVERRIDE + "\n")
    assert packet.make("destroy", "TARGET=dev_fast").returncode == 0
    assert packet.calls() == ["bundle destroy -t dev_fast"]
    assert packet.vars_file.read_text().strip() == OVERRIDE
    assert packet.make("simulate").returncode == 0
    assert packet.calls()[-1] == f"bundle run -t dev {OVERRIDE} simulate_system_tables"


def test_local_targets_ignore_saved_vars(packet):
    packet.vars_file.write_text(OVERRIDE + "\n")
    result = packet.make("help")
    assert result.returncode == 0
    assert "Using saved VARS" not in result.stdout
    assert not packet.log.exists()


# ---------------------------------------------------------------------------
# Pre-flight: missing CLI and sign-in failures
# ---------------------------------------------------------------------------

def test_missing_cli_says_how_to_install(packet, tmp_path):
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    result = packet.make("deploy", path=str(empty))
    assert result.returncode != 0
    assert "Databricks CLI not found" in result.stdout


def test_default_profile_failure_lists_profile_names(packet):
    result = packet.make("deploy", FAKE_AUTH_FAIL="1")
    assert result.returncode != 0
    assert "can't sign in with its DEFAULT profile" in result.stdout
    assert "  alpha\n  beta\n" in result.stdout
    assert "Valid" not in result.stdout, "listing should show names only"
    assert "make deploy PROFILE=<name>" in result.stdout
    assert packet.calls() == [], "nothing should deploy after a failed sign-in"


def test_unknown_profile_lists_profile_names(packet):
    result = packet.make("validate", "PROFILE=nope", FAKE_AUTH_FAIL="1")
    assert result.returncode != 0
    assert "No Databricks CLI profile named 'nope'" in result.stdout
    assert "  alpha\n  beta\n" in result.stdout


def test_expired_profile_says_how_to_log_in(packet):
    result = packet.make("validate", "PROFILE=beta", FAKE_AUTH_FAIL="1")
    assert result.returncode != 0
    assert "databricks auth login --profile beta" in result.stdout


def test_profile_listing_skips_network_validation(packet):
    packet.make("validate", "PROFILE=nope", FAKE_AUTH_FAIL="1")
    profile_calls = [c for c in packet.all_calls() if c.startswith("auth profiles")]
    assert profile_calls, "expected the pre-flight to list profiles"
    assert all("--skip-validate" in c for c in profile_calls)


# ---------------------------------------------------------------------------
# Local test targets need a finished `make setup`
# ---------------------------------------------------------------------------

def test_missing_venv_says_run_setup(packet):
    result = packet.make("test-legacy")
    assert result.returncode != 0
    assert "Run 'make setup' first" in result.stdout


def test_unfinished_setup_says_rerun_setup(packet):
    venv_python = packet.dir / ".venv" / "bin" / "python"
    venv_python.parent.mkdir(parents=True)
    venv_python.write_text("#!/bin/sh\nexit 1\n")  # a venv whose packages never installed
    venv_python.chmod(0o755)
    result = packet.make("test-legacy")
    assert result.returncode != 0
    assert "Re-run 'make setup'" in result.stdout
