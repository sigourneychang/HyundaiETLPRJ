#!/usr/bin/env bash
# Run beeline against the src or tgt Hive 2 cluster (docker/docker-compose.yml).
#   ./scripts/beeline.sh src                       -> interactive shell on src cluster
#   ./scripts/beeline.sh tgt -e "show databases;"  -> run a query on tgt cluster
#   ./scripts/beeline.sh src -f path/to/file.sql   -> run a local SQL file
set -euo pipefail
[ -x /opt/homebrew/bin/brew ] && eval "$(/opt/homebrew/bin/brew shellenv)"

case "${1:-}" in
  src|tgt) CLUSTER="$1"; shift ;;
  *) echo "usage: $0 <src|tgt> [-e SQL | -f FILE | beeline args]" >&2; exit 2 ;;
esac
CONTAINER="${CLUSTER}-hiveserver2"
URL="jdbc:hive2://localhost:10000/"
BEELINE=/opt/hive/bin/beeline

if [ "${1:-}" = "-f" ] && [ -n "${2:-}" ]; then
  # Hive 2 beeline silently skips a multi-statement -e string that starts with a
  # "--" comment, so copy the file in and use a real -f.
  TMP="/tmp/beeline_$$.sql"
  docker cp "$2" "$CONTAINER:$TMP" >/dev/null
  trap 'docker exec "$CONTAINER" rm -f "$TMP" >/dev/null 2>&1 || true' EXIT
  shift 2
  docker exec -i "$CONTAINER" $BEELINE -u "$URL" --silent=true "$@" -f "$TMP"
elif [ $# -gt 0 ]; then
  docker exec -i "$CONTAINER" $BEELINE -u "$URL" --silent=true "$@"
else
  docker exec -it "$CONTAINER" $BEELINE -u "$URL"
fi
