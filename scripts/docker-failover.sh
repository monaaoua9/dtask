#!/usr/bin/env bash
# Real processes, real TCP, real container kills. Needs docker compose.
#   scripts/docker-failover.sh
# Submits tasks (with idempotency keys), kills the leader container, freezes the
# next leader, checks nobody claims leadership without a majority, brings both
# back and checks all tasks were accepted once and finished.
set -uo pipefail
export MSYS_NO_PATHCONV=1

F="docker compose -f deploy/docker-compose.yml"
PEERS=n1:7000,n2:7000,n3:7000
fail() { echo "FAIL: $*" >&2; $F logs --no-color --tail 20 n1 n2 n3 >&2; exit 1; }

# dtaskctl from a throwaway container on the cluster network (plain docker run, not
# compose run: a one-off copy of a service steals that service's DNS name)
ctl() { docker run --rm --network deploy_default --entrypoint /usr/local/bin/dtaskctl deploy-n1 --addr "$PEERS" --timeout 20s "$@"; }

role_of() { ctl status --node "$1:7000" 2>/dev/null | sed -n '1s/.*(\(.*\)), term.*/\1/p'; }

find_leader() { # excluding nodes passed as args
  for n in n1 n2 n3; do
    for x in "$@"; do [ "$n" = "$x" ] && continue 2; done
    [ "$(role_of "$n")" = "leader" ] && { echo "$n"; return; }
  done
}

wait_leader() {
  for _ in $(seq 1 60); do l=$(find_leader "$@"); [ -n "$l" ] && { echo "$l"; return 0; }; sleep 1; done
  return 1
}

submit() { # prefix count
  for i in $(seq 1 "$2"); do
    for try in 1 2 3 4 5; do
      err=$(ctl submit --type echo --payload "$1-$i" --key "$1-$i" 2>&1) && continue 2
      sleep 1
    done
    echo "last error: $err" >&2
    fail "submit $1-$i never went through"
  done
}

$F up --build -d n1 n2 n3 >/dev/null 2>&1 || fail "compose up"
trap '$F down -v >/dev/null 2>&1' EXIT

L=$(wait_leader) || fail "no leader after startup"
echo "leader: $L"
submit before 20

echo "killing leader $L"
$F kill "$L" >/dev/null
N=$(wait_leader "$L") || fail "no new leader after killing $L"
echo "new leader: $N"
submit during 20

echo "freezing $N: process alive but silent, like a long pause"
$F pause "$N" >/dev/null
S=""; for n in n1 n2 n3; do [ "$n" != "$L" ] && [ "$n" != "$N" ] && S=$n; done
sleep 10
[ "$(role_of "$S")" != "leader" ] || fail "$S is leader with only one node reachable"
echo "$S correctly refuses leadership without a majority"

$F unpause "$N" >/dev/null
$F start "$L" >/dev/null
M=$(wait_leader) || fail "cluster did not recover"
echo "recovered, leader: $M"
submit after 20

for _ in $(seq 1 60); do
  done_n=$(ctl --json list --state succeeded --limit 1000 2>/dev/null | grep -c '"idempotencyKey"')
  [ "$done_n" -ge 60 ] && break; sleep 2
done
echo "succeeded: $done_n of 60"
[ "$done_n" -eq 60 ] || fail "expected exactly 60 succeeded tasks, got $done_n"
echo "PASS"
