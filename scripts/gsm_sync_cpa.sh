#!/usr/bin/env bash
set -eo pipefail

# Unified CPA sync helper for Google Secret Manager.
# Modes:
#   pull: cloud -> local
#   push: local -> cloud
#   both: pull then push (default)
#
# Usage:
#   scripts/gsm_sync_cpa.sh [pull|push|both] [--project PROJECT_ID] [--prefix PREFIX] [--auth-dir DIR] [--dry-run]

MODE="both"
PROJECT_ARG=()
PREFIX_ARG=()
AUTH_ARG=()
DRY_ARG=()

if [[ $# -gt 0 ]]; then
  case "$1" in
    pull|push|both)
      MODE="$1"
      shift
      ;;
  esac
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_ARG=(--project "${2:-}")
      shift 2
      ;;
    --prefix)
      PREFIX_ARG=(--prefix "${2:-}")
      shift 2
      ;;
    --auth-dir)
      AUTH_ARG=(--auth-dir "${2:-}")
      shift 2
      ;;
    --dry-run)
      DRY_ARG=(--dry-run)
      shift
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATE="$SCRIPT_DIR/gsm_migrate_cpa.sh"
RESTORE="$SCRIPT_DIR/gsm_restore_cpa.sh"

if [[ ! -x "$MIGRATE" || ! -x "$RESTORE" ]]; then
  echo "Missing executable scripts: gsm_migrate_cpa.sh / gsm_restore_cpa.sh" >&2
  exit 1
fi

run_pull() {
  "$RESTORE" "${PROJECT_ARG[@]}" "${PREFIX_ARG[@]}" "${AUTH_ARG[@]}" "${DRY_ARG[@]}"
}

run_push() {
  "$MIGRATE" "${PROJECT_ARG[@]}" "${PREFIX_ARG[@]}" "${DRY_ARG[@]}"
}

case "$MODE" in
  pull)
    run_pull
    ;;
  push)
    run_push
    ;;
  both)
    run_pull
    run_push
    ;;
  *)
    echo "Invalid mode: $MODE" >&2
    exit 2
    ;;
esac
