# DSA Interview Packet: local tests and bundle shortcuts.
# Run every target from this directory. `make help` lists them.
#
# Overridable variables:
#   PYTHON   interpreter used to create the virtualenv (default: python3; needs 3.11+)
#   PROFILE  Databricks CLI profile (default: none, so the CLI uses its DEFAULT profile)
#   TARGET   bundle target: dev or dev_fast (default: dev)
#   VARS     extra bundle flags, e.g. VARS='--var=catalog=my_catalog --var=schema=my_schema'.
#            A successful deploy saves them to .bundle-vars-<TARGET> and later commands reuse them;
#            pass VARS again to replace them, or VARS= to go back to the defaults.
#   AUTO_APPROVE  set to exactly 1 so `make destroy` skips its confirmation prompt (needed outside a terminal)
#   TESTS    which tests `make test` runs (default: tests/), e.g. TESTS=tests/test_lakehouse.py
#   ARGS     extra pytest flags, e.g. ARGS='-k pricing_summary -x'

PYTHON ?= python3
VENV ?= .venv
VENV_PY := $(VENV)/bin/python
PROFILE ?=
TARGET ?= dev
TESTS ?= tests/
ARGS ?=

BUNDLE = databricks bundle
PROFILE_FLAG = $(if $(PROFILE),--profile $(PROFILE))
BUNDLE_FLAGS = -t $(TARGET) $(PROFILE_FLAG) $(VARS)
JAVA = $${JAVA_HOME:+$$JAVA_HOME/bin/}java

# VARS from the command line is saved after a successful deploy; otherwise reuse the saved value.
VARS_FILE = .bundle-vars-$(TARGET)
ifneq ($(filter command line environment,$(origin VARS)),)
SAVE_VARS = $(if $(strip $(VARS)),printf '%s\n' '$(VARS)' > $(VARS_FILE),rm -f $(VARS_FILE))
else
VARS := $(shell cat $(VARS_FILE) 2>/dev/null)
VARS_NOTE = $(if $(VARS),echo "Using saved VARS from $(VARS_FILE): $(VARS) (pass VARS= to reset)")
endif

.PHONY: help setup check-java test test-legacy test-lakehouse test-lakeflow \
	validate deploy seed simulate ml init integration destroy clean \
	_require-venv _require-workspace

help:
	@echo "DSA Interview Packet - available targets:"
	@echo ""
	@echo "Local (no Databricks needed; Python 3.11+ and JDK 17 or 21):"
	@echo "  setup           Create $(VENV) and install pinned test dependencies"
	@echo "  check-java      Confirm a JDK 17 or 21 is available for local Spark"
	@echo "  test            Run the test suite (TESTS=<path or node> to narrow, ARGS for pytest flags)"
	@echo "  test-legacy     Legacy pandas tests only (pass out of the box)"
	@echo "  test-lakehouse  Everything except Lakeflow tests; Lakehouse tests skip until you add SQL files"
	@echo "  test-lakeflow   Everything except Lakehouse tests; Lakeflow tests skip until you add functions"
	@echo ""
	@echo "Workspace (Databricks CLI >= 0.260.0; PROFILE, TARGET, VARS apply):"
	@echo "  validate        databricks bundle validate"
	@echo "  deploy          databricks bundle deploy"
	@echo "  seed            Deploy, then run seed_source_tables"
	@echo "  simulate        Deploy, then run simulate_system_tables"
	@echo "  ml              Deploy, then run ml_train_eval_pipeline"
	@echo "  init            deploy, then seed -> simulate -> ml (full cold start)"
	@echo "  integration     Deploy your current files, then run run_integration_tests"
	@echo "  destroy         databricks bundle destroy (tables and models stay; see SETUP.md)"
	@echo "                  asks to confirm; AUTO_APPROVE=1 skips that (e.g. from an AI agent)"
	@echo ""
	@echo "  VARS is saved by a successful deploy and reused; pass VARS= to reset to the defaults."
	@echo ""
	@echo "  clean           Remove $(VENV) and test caches"

$(VENV_PY):
	@$(PYTHON) -c 'import sys' >/dev/null 2>&1 || { \
		echo "Cannot run '$(PYTHON)'. Install Python 3.11+ and point PYTHON at it,"; \
		echo "e.g. make setup PYTHON=/path/to/python3.11"; exit 1; }
	@$(PYTHON) -c 'import sys; sys.exit(sys.version_info < (3, 11))' || { \
		echo "Python 3.11+ is required ('$(PYTHON)' is $$($(PYTHON) --version 2>&1))."; \
		echo "Re-run with a newer interpreter, e.g. make setup PYTHON=/path/to/python3.11"; exit 1; }
	$(PYTHON) -m venv $(VENV)

# Shell snippet shared by check-java (fails on a bad JDK) and setup (only warns).
CHECK_JAVA = \
	command -v $(JAVA) >/dev/null 2>&1 || { \
		echo "Java not found. Install JDK 17 or 21 and put it on PATH (or set JAVA_HOME)."; exit 1; }; \
	v=$$($(JAVA) -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/ {print $$2}'); \
	case "$$v" in \
		17|21) echo "Java $$v: OK for local Spark tests." ;; \
		"") echo "No working JDK found. Install JDK 17 or 21 and put it on PATH (or set JAVA_HOME)."; exit 1 ;; \
		*) echo "Java $$v found, but local Spark tests need JDK 17 or 21. Point JAVA_HOME at one."; exit 1 ;; \
	esac

setup: $(VENV_PY)
	$(VENV_PY) -m pip install --upgrade pip
	$(VENV_PY) -m pip install -r requirements.txt
	@echo ""
	@echo "Setup complete: $(VENV) is ready."
	@( $(CHECK_JAVA) ) || echo "Fix the JDK before 'make test'; 'make test-legacy' works without it."

check-java:
	@$(CHECK_JAVA)

_require-venv:
	@test -x $(VENV_PY) || { echo "No virtualenv at $(VENV). Run 'make setup' first."; exit 1; }
	@$(VENV_PY) -c 'import pytest, pandas, pyspark' 2>/dev/null || { \
		echo "$(VENV) is missing test dependencies (did 'make setup' finish?). Re-run 'make setup'."; exit 1; }

test: _require-venv check-java
	$(VENV_PY) -m pytest $(TESTS) $(ARGS)

test-legacy: _require-venv
	$(VENV_PY) -m pytest tests/test_legacy.py tests/test_legacy_smoke.py $(ARGS)

test-lakehouse: _require-venv check-java
	$(VENV_PY) -m pytest tests/ --impl lakehouse $(ARGS)

test-lakeflow: _require-venv check-java
	$(VENV_PY) -m pytest tests/ --impl lakeflow $(ARGS)

# Fail fast with a clear next step when the CLI is missing or can't authenticate
# (the CLI's own "cannot configure default credentials" error doesn't say what to do).
_require-workspace:
	@command -v databricks >/dev/null 2>&1 || { \
		echo "Databricks CLI not found. Install it (>= 0.260.0): https://docs.databricks.com/dev-tools/cli/install.html"; exit 1; }
	@databricks current-user me $(PROFILE_FLAG) >/dev/null 2>&1 || { \
		if [ -n "$(PROFILE)" ] && databricks auth profiles --skip-validate 2>/dev/null | awk 'NR>1 {print $$1}' | grep -qx '$(PROFILE)'; then \
			echo "The Databricks CLI can't sign in with profile '$(PROFILE)' (login expired?). Re-authenticate with:"; \
			echo "  databricks auth login --profile $(PROFILE)"; \
		elif [ -n "$(PROFILE)" ]; then \
			echo "No Databricks CLI profile named '$(PROFILE)'. Saved profiles:"; \
			databricks auth profiles --skip-validate 2>/dev/null | awk 'NR>1 {print "  " $$1}'; \
		else \
			echo "The Databricks CLI can't sign in with its DEFAULT profile. Saved profiles:"; \
			databricks auth profiles --skip-validate 2>/dev/null | awk 'NR>1 {print "  " $$1}'; \
			echo "Re-run with your profile, e.g. make $(firstword $(MAKECMDGOALS)) PROFILE=<name>,"; \
			echo "or sign in first: databricks auth login --host https://<your-workspace-url>"; \
		fi; exit 1; }
	@$(VARS_NOTE)

validate: _require-workspace
	$(BUNDLE) validate $(BUNDLE_FLAGS)

deploy: _require-workspace
	$(BUNDLE) deploy $(BUNDLE_FLAGS)
	@$(SAVE_VARS)

# Job targets deploy first so the workspace always runs your current local files
# (make deploys once per invocation, e.g. `make seed simulate ml` deploys a single time).
RUN = $(BUNDLE) run $(BUNDLE_FLAGS)

# Only the documented value skips destroy's confirmation prompt.
AUTO_APPROVED = $(filter 1,$(AUTO_APPROVE))
DESTROY_HINT = echo "Destroy did not run. It asks for confirmation, so run it in your own terminal,"; \
	echo "or, once you're sure, skip the prompt with: make destroy AUTO_APPROVE=1";

seed: deploy
	$(RUN) seed_source_tables

simulate: deploy
	$(RUN) simulate_system_tables

ml: deploy
	$(RUN) ml_train_eval_pipeline

init: deploy
	$(RUN) seed_source_tables
	$(RUN) simulate_system_tables
	$(RUN) ml_train_eval_pipeline

integration: deploy
	$(RUN) run_integration_tests

destroy: _require-workspace
	@$(BUNDLE) destroy $(BUNDLE_FLAGS) $(if $(AUTO_APPROVED),--auto-approve) || { \
		$(if $(AUTO_APPROVED),,$(DESTROY_HINT)) exit 1; }
	rm -f $(VARS_FILE)

clean:
	rm -rf $(VENV) .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
