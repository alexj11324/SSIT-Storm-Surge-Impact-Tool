#!/usr/bin/env bash
set -euo pipefail

# Migrate local CPA/Quotio credentials into Google Secret Manager.
# Uploads:
#   - quotio-config-yaml (from local config.yaml)
#   - claude-settings-json (from ~/.claude/settings.json, if present)
#   - codmate-config-yaml (from CodMate config.yaml, if present)
#   - cpa-auth-<normalized_name> (one per auth JSON)
#
# Usage:
#   scripts/gsm_migrate_cpa.sh [--project PROJECT_ID] [--prefix PREFIX] [--dry-run]

PROJECT_ID=""
PREFIX="cpa"
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

CONFIG_PATH=""
for p in \
  "$HOME/Library/Application Support/Quotio/config.yaml" \
  "$HOME/Library/Application Support/CodMate/config.yaml"; do
  if [[ -f "$p" ]]; then
    CONFIG_PATH="$p"
    break
  fi
done

if [[ -z "$CONFIG_PATH" ]]; then
  echo "No local config.yaml found under Quotio/CodMate." >&2
  exit 1
fi

AUTH_DIR=""
for d in \
  "$HOME/.cli-proxy-api" \
  "$HOME/Library/Application Support/Quotio/auths" \
  "$HOME/Library/Application Support/CodMate/auths"; do
  if [[ -d "$d" ]]; then
    AUTH_DIR="$d"
    break
  fi
done

if [[ -z "$AUTH_DIR" ]]; then
  echo "No auth directory found (~/.cli-proxy-api or Quotio/CodMate auths)." >&2
  exit 1
fi

normalize_name() {
  local s="$1"
  s="$(printf '%s' "$s" | tr '[:upper:]' '[:lower:]')"
  s="${s//[^a-z0-9-]/-}"
  s="$(echo "$s" | sed -E 's/-+/-/g; s/^-+//; s/-+$//')"
  echo "$s"
}

upsert_secret_version() {
  local sid="$1"
  local file="$2"

  if ! gcloud secrets describe "$sid" --project "$PROJECT_ID" >/dev/null 2>&1; then
    if [[ $DRY_RUN -eq 1 ]]; then
      echo "[dry-run] create secret: $sid"
    else
      gcloud secrets create "$sid" \
        --project "$PROJECT_ID" \
        --replication-policy="automatic" >/dev/null
      echo "created: $sid"
    fi
  fi

  if [[ $DRY_RUN -eq 1 ]]; then
    echo "[dry-run] add version: $sid <- $file"
  else
    gcloud secrets versions add "$sid" \
      --project "$PROJECT_ID" \
      --data-file="$file" >/dev/null
    echo "version added: $sid"
  fi
}

CONFIG_SECRET_ID="$(normalize_name "${PREFIX}-quotio-config-yaml")"
CLAUDE_SETTINGS_PATH="$HOME/.claude/settings.json"
CLAUDE_SETTINGS_SECRET_ID="$(normalize_name "${PREFIX}-claude-settings-json")"
CODMATE_CONFIG_PATH="$HOME/Library/Application Support/CodMate/config.yaml"
CODMATE_CONFIG_SECRET_ID="$(normalize_name "${PREFIX}-codmate-config-yaml")"

echo "Project: $PROJECT_ID"
echo "Config : $CONFIG_PATH"
echo "Auths  : $AUTH_DIR"
echo "Prefix : $PREFIX"

echo "Uploading config..."
upsert_secret_version "$CONFIG_SECRET_ID" "$CONFIG_PATH"

if [[ -f "$CLAUDE_SETTINGS_PATH" ]]; then
  echo "Uploading Claude settings..."
  upsert_secret_version "$CLAUDE_SETTINGS_SECRET_ID" "$CLAUDE_SETTINGS_PATH"
fi

if [[ -f "$CODMATE_CONFIG_PATH" ]]; then
  echo "Uploading CodMate config..."
  upsert_secret_version "$CODMATE_CONFIG_SECRET_ID" "$CODMATE_CONFIG_PATH"
fi

auth_count=0
MAP_FILE="$(mktemp)"
while IFS= read -r -d '' jf; do
  base="$(basename "$jf")"
  name="${base%.json}"
  sid="$(normalize_name "${PREFIX}-auth-${name}")"
  # De-duplicate by secret id to avoid uploading twice when both raw and normalized filenames exist.
  if ! grep -q "^${sid}|" "$MAP_FILE"; then
    printf '%s|%s\n' "$sid" "$jf" >> "$MAP_FILE"
  fi
done < <(find "$AUTH_DIR" -maxdepth 1 -type f -name '*.json' -print0)

while IFS='|' read -r sid jf; do
  [[ -z "$sid" || -z "$jf" ]] && continue
  upsert_secret_version "$sid" "$jf"
  auth_count=$((auth_count + 1))
done < "$MAP_FILE"
rm -f "$MAP_FILE"

echo "Done. Uploaded $auth_count auth JSON file(s) + config secrets."
echo "Secrets naming pattern: ${PREFIX}-quotio-config-yaml, ${PREFIX}-codmate-config-yaml, ${PREFIX}-claude-settings-json, ${PREFIX}-auth-*"
