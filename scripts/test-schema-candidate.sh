#!/usr/bin/env bash
# Generate and exercise an unpublished public schema without altering pins/lock.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec python3 "$ROOT/scripts/schema-candidate.py" "$@"
