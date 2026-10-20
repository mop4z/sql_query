#!/usr/bin/env bash
# Runs the Postgres/Redis-backed cache tests against throwaway containers.
#
# Starts one Postgres and one Redis container with the images and Redis
# eviction policy Opal runs in production, on ephemeral host ports, runs the
# ignored integration tests serially, and removes both containers on exit —
# including on failure or interrupt. Never reuses a running service.
#
#   bash scripts/cache-tests.sh                       # cache_fence suite
#   bash scripts/cache-tests.sh --test cache_measure  # any cargo test selector
#
# Extra arguments replace the default `--test cache_fence`; arguments after
# `--` go to the test binary.
set -euo pipefail

cd "$(dirname "$0")/.."

PG_IMAGE=postgres:18.2
REDIS_IMAGE=redis:8.6.2-alpine
id="sqlq-cache-$$-$(date +%s)"
pg="$id-pg"
rd="$id-redis"

cleanup()
{
    docker rm -f "$pg" "$rd" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker run -d --rm --name "$pg" -e POSTGRES_PASSWORD=sqlq -e POSTGRES_DB=sqlq \
    -p 127.0.0.1::5432 "$PG_IMAGE" >/dev/null
docker run -d --rm --name "$rd" -p 127.0.0.1::6379 "$REDIS_IMAGE" \
    redis-server --save '' --appendonly no --maxmemory 512mb --maxmemory-policy allkeys-lru >/dev/null

pg_port=$(docker port "$pg" 5432/tcp | head -n1 | sed 's/.*://')
rd_port=$(docker port "$rd" 6379/tcp | head -n1 | sed 's/.*://')

for _ in $(seq 60); do
    if docker exec "$pg" pg_isready -U postgres -d sqlq >/dev/null 2>&1 \
        && docker exec "$rd" redis-cli ping >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done
docker exec "$pg" pg_isready -U postgres -d sqlq >/dev/null
docker exec "$rd" redis-cli ping >/dev/null

export SQL_QUERY_TEST_DATABASE_URL="postgres://postgres:sqlq@127.0.0.1:$pg_port/sqlq"
export SQL_QUERY_TEST_REDIS_URL="redis://127.0.0.1:$rd_port/0"
echo "isolated instance: $pg (:$pg_port), $rd (:$rd_port)"

selector=()
while [[ $# -gt 0 && "$1" != "--" ]]; do selector+=("$1"); shift; done
[[ "${1:-}" == "--" ]] && shift
[[ ${#selector[@]} -eq 0 ]] && selector=(--test cache_fence)

cargo test --features test-support "${selector[@]}" -- --ignored --test-threads=1 "$@"
