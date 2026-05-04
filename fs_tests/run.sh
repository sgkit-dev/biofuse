#!/usr/bin/env bash
# Front-end for the biofuse fs_tests harness.
#
# Usage:
#   ./fs_tests/run.sh [--quick] [--large] [--results DIR] [SUBCOMMAND] [SUBCOMMAND_ARGS...]
#
# Subcommands: posix | pjdfstest | fio | stress-ng | lifecycle | all (default).
set -euo pipefail

script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
export PYTHONPATH="$script_dir:${PYTHONPATH:-}"
cd "$repo_root"
exec uv run --group fs-tests python -m harness "$@"
