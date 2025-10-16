#!/usr/bin/env bash
set -euo pipefail

ROOT_PATH="${ROOT_PATH:-$(pwd)}"
cd "$ROOT_PATH"

echo "Working directory: $ROOT_PATH"

# -------------------------------
# Duplicate includes check
# -------------------------------
echo "Running duplicate includes check..."
DUP_OUTPUT=$(mktemp)
if ./utils/check-style/check-duplicate-includes.sh 2>&1 | tee "$DUP_OUTPUT"; then
    :
fi

if [[ -s "$DUP_OUTPUT" ]]; then
    echo "Duplicate includes check: FAILED"
    echo "===== Duplicate includes details ====="
    cat "$DUP_OUTPUT"
    echo "====================================="
else
    echo "Duplicate includes check: OK"
fi

# -------------------------------
# Style check
# -------------------------------
echo "Running style check..."
STYLE_OUTPUT=$(mktemp)
if ./utils/check-style/check-style 2>&1 | tee "$STYLE_OUTPUT"; then
    :
fi

if [[ -s "$STYLE_OUTPUT" ]]; then
    echo "Style check: FAILED"
    echo "===== Style check details ====="
    cat "$STYLE_OUTPUT"
    echo "==============================="
else
    echo "Style check: OK"
fi

# -------------------------------
# Unified result
# -------------------------------
if [[ -s "$DUP_OUTPUT" || -s "$STYLE_OUTPUT" ]]; then
    echo "One or more checks failed. Exiting with error."
    rm -f "$DUP_OUTPUT" "$STYLE_OUTPUT"
    exit 1
fi

rm -f "$DUP_OUTPUT" "$STYLE_OUTPUT"
echo "Checking completed!"