#!/usr/bin/env zsh
# Helper commands for running the unified ingestor and simulator on macOS (zsh)
#
# Usage examples:
#   # 1) Activate venv and verify
#   ./commands.sh act
#
#   # 2) Install/update dependencies into the venv
#   ./commands.sh deps
#
#   # 3) Run the ingestor (blocks; stop with Ctrl+C)
#   ./commands.sh run_app
#
#   # 4) Run the simulator interactively
#   ./commands.sh sim
#
#   # 5) Run the simulator non-interactively (DeviceID=84, 10 P fields, 20 messages, 300ms interval)
#   ./commands.sh sim_ni 84 10 20 300
#
# Notes:
# - Expects an existing venv at <project-root>/.venv
# - Project root is resolved relative to this script’s location.
# - All commands run from the project root so relative paths work.

set -euo pipefail

# Resolve project root (this script is at unified_ingestor/tools/commands.sh)
SCRIPT_DIR=${0:A:h}
PROJECT_ROOT="${SCRIPT_DIR}/../.."
VENV_ACTIVATE="${PROJECT_ROOT}/.venv/bin/activate"
REQ_FILE="${PROJECT_ROOT}/unified_ingestor/requirements.txt"

require_venv() {
  if [[ ! -f "${VENV_ACTIVATE}" ]]; then
    echo "Error: venv not found at ${VENV_ACTIVATE}" >&2
    echo "Create it with: python3 -m venv '${PROJECT_ROOT}/.venv'" >&2
    exit 1
  fi
}

act() {
  require_venv
  # shellcheck disable=SC1090
  source "${VENV_ACTIVATE}"
  python -V
}

deps() {
  require_venv
  # shellcheck disable=SC1090
  source "${VENV_ACTIVATE}"
  if [[ -f "${REQ_FILE}" ]]; then
    python -m pip install -r "${REQ_FILE}"
  else
    echo "Warning: requirements file not found at ${REQ_FILE}" >&2
  fi
}

run_app() {
  require_venv
  # shellcheck disable=SC1090
  source "${VENV_ACTIVATE}"
  cd "${PROJECT_ROOT}"
  python -m unified_ingestor.main
}

sim() {
  require_venv
  # shellcheck disable=SC1090
  source "${VENV_ACTIVATE}"
  cd "${PROJECT_ROOT}"
  python unified_ingestor/tools/gree1_simulator.py "$@"
}

# Non-interactive simulator wrapper
# Args: <deviceId> <pfields> [count=10] [interval_ms=300]
# Example: sim_ni 84 10 20 300
sim_ni() {
  local dev="${1:-84}"
  local pfields="${2:-10}"
  local count="${3:-10}"
  local interval="${4:-300}"
  sim --device "${dev}" --pfields "${pfields}" --count "${count}" --interval "${interval}"
}

help() {
  grep -E "^#( |$)|^[a-z_]+\(\)" "${0}" | sed 's/^# \{0,1\}//'
}

main() {
  local cmd="${1:-help}"
  shift || true
  case "${cmd}" in
    act|deps|run_app|sim|sim_ni|help)
      "${cmd}" "$@" ;;
    *)
      echo "Unknown command: ${cmd}" >&2
      help
      exit 2 ;;
  esac
}

main "$@"
