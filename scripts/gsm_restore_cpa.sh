#!/usr/bin/env bash
set -euo pipefail

# Restore CPA/Quotio credentials from Google Secret Manager to local files.
# Restores:
#   - <prefix>-quotio-config-yaml -> Quotio config.yaml
#   - <prefix>-codmate-config-yaml -> CodMate config.yaml (if secret exists)
#   - <prefix>-claude-settings-json -> ~/.claude/settings.json (if secret exists)
#   - <prefix>-auth-* secrets      -> ~/.cli-proxy-api/*.json (default)
#
# Usage:
#   scripts/gsm_restore_cpa.sh [--project PROJECT_ID] [--prefix PREFIX] [--auth-dir DIR] [--dry-run]

PROJECT_ID=""
PREFIX="cpa"
AUTH_DIR="$HOME/.cli-proxy-api"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_ID="${2:-}"
      shift 2
      ;;
    --prefix)
      PREFIX="${2:-}"
      shift 2
      ;;
    --auth-dir)
      AUTH_DIR="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud is required." >&2
  exit 1
fi

if [[ -z "$PROJECT_ID" ]]; then
  PROJECT_ID="$(gcloud config get-value project 2>/dev/null || true)"
fi

if [[ -z "$PROJECT_ID" ]]; then
  echo "GCP project is missing. Pass --project PROJECT_ID." >&2
  exit 1
fi

CONFIG_PATH="$HOME/Library/Application Support/Quotio/config.yaml"
CODMATE_CONFIG_PATH="$HOME/Library/Application Support/CodMate/config.yaml"
CLAUDE_SETTINGS_PATH="$HOME/.claude/settings.json"
mkdir -p "$(dirname "$CONFIG_PATH")"
mkdir -p "$(dirname "$CODMATE_CONFIG_PATH")"
mkdir -p "$(dirname "$CLAUDE_SETTINGS_PATH")"
mkdir -p "$AUTH_DIR"

normalize_name() {
  local s="$1"
  s="$(printf '%s' "$s" | tr '[:upper:]' '[:lower:]')"
  s="${s//[^a-z0-9-]/-}"
  s="$(echo "$s" | sed -E 's/-+/-/g; s/^-+//; s/-+$//')"
  echo "$s"
}

resolve_auth_output_path() {
  local suffix="$1"
  local candidate=""
  local b=""
  for f in "$AUTH_DIR"/*.json; do
    [ -f "$f" ] || continue
    b="$(basename "$f" .json)"
    if [[ "$(normalize_name "$b")" == "$suffix" ]]; then
      candidate="$f"
      break
    fi
  done
  if [[ -n "$candidate" ]]; then
    echo "$candidate"
  else
    echo "$AUTH_DIR/${suffix}.json"
  fi
}

CONFIG_SECRET_ID="$(normalize_name "${PREFIX}-quotio-config-yaml")"
CODMATE_SECRET_ID="$(normalize_name "${PREFIX}-codmate-config-yaml")"
CLAUDE_SETTINGS_SECRET_ID="$(normalize_name "${PREFIX}-claude-settings-json")"
AUTH_PREFIX_ID="$(normalize_name "${PREFIX}-auth")"

echo "Project: $PROJECT_ID"
echo "Restore config -> $CONFIG_PATH"
echo "Restore codmate -> $CODMATE_CONFIG_PATH"
echo "Restore claude  -> $CLAUDE_SETTINGS_PATH"
echo "Restore auths  -> $AUTH_DIR"

if [[ $DRY_RUN -eq 1 ]]; then
  echo "[dry-run] access secret: $CONFIG_SECRET_ID"
else
  gcloud secrets versions access latest \
    --secret "$CONFIG_SECRET_ID" \
    --project "$PROJECT_ID" > "$CONFIG_PATH"
  chmod 600 "$CONFIG_PATH"
  echo "restored config"
fi

restore_optional_secret() {
  local sid="$1"
  local out="$2"
  if gcloud secrets describe "$sid" --project "$PROJECT_ID" >/dev/null 2>&1; then
    if [[ $DRY_RUN -eq 1 ]]; then
      echo "[dry-run] access secret: $sid -> $out"
    else
      gcloud secrets versions access latest \
        --secret "$sid" \
        --project "$PROJECT_ID" > "$out"
      chmod 600 "$out"
      echo "restored optional: $sid"
    fi
  else
    echo "skip optional secret (not found): $sid"
  fi
}

restore_optional_secret "$CODMATE_SECRET_ID" "$CODMATE_CONFIG_PATH"
restore_optional_secret "$CLAUDE_SETTINGS_SECRET_ID" "$CLAUDE_SETTINGS_PATH"

restored=0
while IFS= read -r sid; do
  [[ -z "$sid" ]] && continue
  suffix="${sid#${AUTH_PREFIX_ID}-}"
  out="$(resolve_auth_output_path "$suffix")"
  if [[ $DRY_RUN -eq 1 ]]; then
    echo "[dry-run] access secret: $sid -> $out"
  else
    gcloud secrets versions access latest \
      --secret "$sid" \
      --project "$PROJECT_ID" > "$out"
    chmod 600 "$out"
    restored=$((restored + 1))
  fi
done < <(gcloud secrets list \
  --project "$PROJECT_ID" \
  --format='value(name)' | sed -E 's#.*/##' | grep -E "^${AUTH_PREFIX_ID}-" || true)

echo "Done. Restored $restored auth JSON file(s)."
