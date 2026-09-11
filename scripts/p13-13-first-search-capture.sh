#!/usr/bin/env bash
#
# p13-13-first-search-capture.sh — one-shot first-search reproduction harness
# for the miroir p13_13 cache-flow investigation (bead miroir-e70cb700,
# authoring half of miroir-b6b71143's empirical capture). Committed verbatim
# from ~/scratch/p13_13_first_search_capture.sh, which drove the three-cycle
# signature-A reproduction of 2026-09-11; the captured evidence and its
# attribution live in docs/notes/p13-13-final-signature-a-capture-2026-09-11.md
# (root cause: docs/notes/p13-13-root-cause-diagnosis.md).
#
# TOPOLOGY — exactly what crates/miroir-proxy/tests/p13_13_cache_flow_integration.rs
# spawns: one getmeili/meilisearch:v1.8.3 node plus the compiled miroir-proxy
# (target/debug/miroir-proxy by default; client 17770, metrics 9090). Against
# that single proxy process it then captures, verbatim:
#
#   1. POST /indexes/products/search   (native: the path the p13_13 suite POSTs)
#   2. POST /search/products           (control: the path routes/search.rs registers)
#
# both requests carry Authorization: Bearer test_master_key and the body
# {"q":"laptop","limit":10}. Status line, headers and body of each response
# are teed to a timestamped log under
# scripts/p13-13-first-search-capture.logs/ (override: P13_13_CAPTURE_LOG_DIR),
# then everything this script spawned is torn down. Nothing else is touched.
#
# FIXED-PORT PRE-FLIGHT (why: docs/notes/p13-13-root-cause-diagnosis.md). The
# proxy hard-binds 17770/9090 and its PROXY_SLOT serializes only one process,
# so before spawning anything the script runs `ss -ltnp` and aborts if either
# port is held: a stray foreign miroir-proxy on the port PASSES the readiness
# check (its /health answers 200 with the exact expected body), so a poisoned
# run looks perfectly healthy and the 404/200 fingerprint is garbage. Held-port
# holders are classified before any action:
#   * ancestry includes a live claude/needle process  -> LIVE WORKER: abort,
#     never kill.
#   * parent chain ends at `systemd --user`, holder start predates every
#     live claude/needle session (no matching live session could have
#     spawned it), and its request log shows no miroir.request line for
#     30+ min while health/pruner ticks continue -> OWNERLESS: SIGTERM it,
#     wait for the ports, proceed.
#   * anything else (undiscoverable pid, unreadable logs, ambiguous) -> abort.
# All three ownerless conditions are required; any failure aborts.
#
# RUNTIME + CREDENTIALS — the node runs on the LOCAL docker.sock: any
# inherited DOCKER_HOST is unset so the container cannot be silently
# redirected to another daemon, and the node key is passed via the
# MEILI_MASTER_KEY environment variable ONLY (the image ENTRYPOINT is tini;
# the CLI-arg form dies with exit 127).
#
# Usage:
#   scripts/p13-13-first-search-capture.sh           # full capture run
#   scripts/p13-13-first-search-capture.sh --check   # pre-flight only: classify
#                                                    # any port holders, take no
#                                                    # action, spawn nothing
#
# Exit codes: 0 capture complete (or --check: ports free); 3 port-conflict
# abort; 1 any other failure; 2 usage.
#
# Env overrides: MIROIR_REPO_DIR (default: repo root inferred from this
# script's location), P13_13_CAPTURE_LOG_DIR (default:
# $MIROIR_REPO_DIR/scripts/p13-13-first-search-capture.logs),
# MIROIR_PROXY_BIN (default $MIROIR_REPO_DIR/target/{debug,release}/miroir-proxy,
# built with `cargo build -p miroir-proxy` if absent).

set -uo pipefail

# --- Suite contract constants (mirrors the p13_13 test file) ----------------
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_DIR="${MIROIR_REPO_DIR:-"$(cd "$SCRIPT_DIR/.." && pwd)"}"
readonly CLIENT_PORT=17770
readonly METRICS_PORT=9090
readonly PROXY_MASTER_KEY="test_master_key"   # client-facing key the generated config carries
readonly NODE_MASTER_KEY="key0"               # shared node key (env API only — the image
                                              # ENTRYPOINT is tini; CLI args die with exit 127)
readonly MEILI_IMAGE="getmeili/meilisearch:v1.8.3"  # tag pinned by testcontainers-modules 0.11
readonly MEILI_INTERNAL_PORT=7700
readonly READY_TIMEOUT_SECS=30                # suite's wait_for_ready deadline
readonly OWNERLESS_IDLE_SECS=1800             # no request-log lines for 30+ min
readonly LOG_FRESH_SECS=600                   # ...while tick lines continue (5-min cadence)
readonly TERM_GRACE_SECS=15
readonly QUERY_BODY='{"q":"laptop","limit":10}'
readonly HEALTH_BODY='{"status":"available"}' # 200, not 204 (routes/health.rs)

readonly LOG_DIR="${P13_13_CAPTURE_LOG_DIR:-$REPO_DIR/scripts/p13-13-first-search-capture.logs}"
readonly RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
readonly MEILI_NAME="p13_13-capture-meili-${RUN_TS}"

CFG_DIR=""
PROXY_PID=""
MEILI_CID=""
LOG_FILE=""

CHECK_ONLY=0
case "${1:-}" in
  "")          ;;
  --check)     CHECK_ONLY=1 ;;
  *) printf 'usage: %s [--check]\n' "$0" >&2; exit 2 ;;
esac

log()  { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }
die()  { log "ABORT: $*"; exit 1; }
port_abort() { log "ABORT (port conflict): $*"; exit 3; }

require_tools() {
  local t
  for t in ss curl docker jq; do
    command -v "$t" >/dev/null 2>&1 || die "required tool missing: $t"
  done
}

# --- Cleanup: tear down ONLY what this script spawned -----------------------
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  if [[ -n "$PROXY_PID" ]] && kill -0 "$PROXY_PID" 2>/dev/null; then
    kill -TERM "$PROXY_PID" 2>/dev/null
    local i
    for i in $(seq 1 $((TERM_GRACE_SECS * 10))); do
      kill -0 "$PROXY_PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$PROXY_PID" 2>/dev/null
  fi
  # Our container only — matched by the unique per-run name we assigned.
  [[ -n "$MEILI_CID" ]] && docker rm -f "$MEILI_CID" >/dev/null 2>&1
  # Preserve the proxy's own log lines (miroir.request records + health/pruner
  # ticks) before the config dir goes away — the capture log alone carries no
  # request-side evidence. Survives aborts too, since cleanup always runs.
  if [[ -n "$CFG_DIR" && -d "$LOG_DIR" ]]; then
    cat "$CFG_DIR/proxy.stdout.log" "$CFG_DIR/proxy.stderr.log" \
      > "$LOG_DIR/proxy-logs-$RUN_TS.log" 2>/dev/null || true
  fi
  [[ -n "$CFG_DIR"   ]] && rm -rf "$CFG_DIR"
  [[ "$rc" -ne 0 ]] && log "cleanup done (exiting rc=$rc)"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --- /proc helpers -----------------------------------------------------------
proc_start_epoch() { stat -Lc %Y "/proc/$1" 2>/dev/null || printf '0'; }
proc_ppid()        { awk '/^PPid:/{print $2}' "/proc/$1/status" 2>/dev/null || printf '0'; }
proc_comm()        { cat "/proc/$1/comm" 2>/dev/null || printf ''; }
proc_cmdline()     { tr '\0' ' ' < "/proc/$1/cmdline" 2>/dev/null || printf ''; }

# Live worker sessions: the claude/needle processes on this box. `pgrep -x`
# matches comm exactly, so a script mentioning "claude" in its args cannot
# false-positive.
live_session_pids() {
  pgrep -x claude 2>/dev/null || true
  pgrep -x needle 2>/dev/null || true
  pgrep -x needle-stable 2>/dev/null || true
}

is_user_systemd() {
  [[ "$(proc_comm "$1")" == "systemd" ]] && proc_cmdline "$1" | grep -q -- '--user'
}

# Walk the holder's parent chain. Prints nothing; sets:
#   ANCESTRY_LIVE=1   if any ancestor is a live claude/needle session
#   CHAIN_USER_MGR=1  if the chain tops out at `systemd --user`
walk_ancestry() {
  ANCESTRY_LIVE=0
  CHAIN_USER_MGR=0
  local p="$1" depth=0 live
  while [[ -n "$p" && "$p" != "0" ]]; do
    for live in $(live_session_pids); do
      if [[ "$live" == "$p" ]]; then ANCESTRY_LIVE=1; return; fi
    done
    is_user_systemd "$p" && { CHAIN_USER_MGR=1; return; }
    [[ "$p" == "1" ]] && return
    p="$(proc_ppid "$p")"
    depth=$((depth + 1))
    [[ $depth -gt 64 ]] && return   # defensive: malformed chain
  done
}

# The holder's own log files (suite convention: proxy.{stdout,stderr}.log in
# its cwd/config dir). lsof is not installed here; /proc/<pid>/fd readlinks
# are the precise equivalent for same-user processes.
holder_log_files() {
  local fd target
  for fd in /proc/"$1"/fd/*; do
    target="$(readlink "$fd" 2>/dev/null)" || continue
    case "$target" in
      *.log) [[ -f "$target" && -r "$target" ]] && printf '%s\n' "$target" ;;
    esac
  done | sort -u
}

# Idle check: last miroir.request line at least OWNERLESS_IDLE_SECS old, while
# the log itself is still being written (some line within LOG_FRESH_SECS —
# pruner/peer-discovery ticks fire every 5 min, so a live proxy's log is never
# 10 min stale). Request lines are the `"target":"miroir.request"` records;
# their embedded ISO-8601 Zulu timestamps sort chronologically as strings.
# Returns 0 = idle-confirmed, 1 = not idle / undeterminable (undeterminable
# aborts — only a confirmed-idle holder is ever signalled).
holder_idle_confirmed() {
  local files f last_req="" last_any="" ts
  files="$(holder_log_files "$1")"
  [[ -z "$files" ]] && return 1   # no readable request log: idle cannot be confirmed -> abort path
  while IFS= read -r f; do
    ts="$(grep '"target":"miroir.request"' "$f" 2>/dev/null \
          | jq -r '.timestamp // empty' 2>/dev/null | sort | tail -n 1)"
    [[ -n "$ts" ]] && { [[ -z "$last_req" || "$ts" > "$last_req" ]] && last_req="$ts"; }
    ts="$(tail -n 200 "$f" 2>/dev/null | jq -r 'select(.timestamp) | .timestamp' 2>/dev/null | sort | tail -n 1)"
    [[ -n "$ts" ]] && { [[ -z "$last_any" || "$ts" > "$last_any" ]] && last_any="$ts"; }
  done <<< "$files"
  [[ -z "$last_any" ]] && return 1   # no parseable timestamps at all
  local now last_any_epoch last_req_epoch
  now="$(date -u +%s)"
  last_any_epoch="$(date -u -d "$last_any" +%s 2>/dev/null)" || return 1
  [[ -n "$last_any_epoch" ]] || return 1
  (( now - last_any_epoch <= LOG_FRESH_SECS )) || return 1   # log gone quiet: not a healthy idle proxy
  if [[ -z "$last_req" ]]; then
    return 0   # served no request ever, yet ticks keep coming: idle by construction
  fi
  last_req_epoch="$(date -u -d "$last_req" +%s 2>/dev/null)" || return 1
  [[ -n "$last_req_epoch" ]] || return 1
  (( now - last_req_epoch >= OWNERLESS_IDLE_SECS ))
}

# Pid of the process LISTENing on a port, from ss's `pid=N` field. Fails (and
# prints nothing) when the port is free OR when the holder pid is not visible
# to this user — the caller aborts on the latter rather than signal blind.
port_holder_pid() {
  local line
  line="$(ss -Hltnp "( sport = :$1 )" 2>/dev/null | head -n 1)"
  [[ -z "$line" ]] && return 1
  local pid
  pid="$(printf '%s\n' "$line" | grep -oE 'pid=[0-9]+' | head -n 1 | cut -d= -f2)"
  [[ -n "$pid" ]] && printf '%s\n' "$pid"
}

# Classify one holder. Prints a verdict block; returns 0 only when the holder
# was confirmed ownerless (and, outside --check, has been SIGTERM'd and the
# port released). Any other outcome is an abort.
resolve_holder() {
  local port="$1" holder
  holder="$(port_holder_pid "$port")" \
    || port_abort "port $port is held but the holder pid is not visible to this user — refusing to proceed blind"
  [[ "$holder" == "$$" ]] && port_abort "port $port is held by this script's own pid — refusing"

  walk_ancestry "$holder"
  local holder_start min_live start
  holder_start="$(proc_start_epoch "$holder")"
  min_live=""
  for start in $(live_session_pids); do
    start="$(proc_start_epoch "$start")"
    [[ "$start" -gt 0 ]] && { [[ -z "$min_live" || "$start" -lt "$min_live" ]] && min_live="$start"; }
  done

  local idle=0
  holder_idle_confirmed "$holder" && idle=1

  log "holder of port $port: pid $holder ($(proc_cmdline "$holder" | cut -c1-120))"
  log "  ancestry includes live claude/needle : $([[ $ANCESTRY_LIVE -eq 1 ]] && echo YES || echo no)"
  log "  parent chain ends at systemd --user  : $([[ $CHAIN_USER_MGR -eq 1 ]] && echo YES || echo no)"
  log "  holder start                         : $(date -u -d "@$holder_start" '+%F %T UTC' 2>/dev/null || echo "$holder_start")"
  log "  oldest live session start            : ${min_live:-none} $([[ -n "$min_live" ]] && date -u -d "@$min_live" '+(%F %T UTC)' 2>/dev/null)"
  log "  predates every live session          : $([[ -n "$min_live" && "$holder_start" -gt 0 && "$holder_start" -lt "$min_live" ]] && echo YES || echo NO)"
  log "  request log idle >=${OWNERLESS_IDLE_SECS}s   : $([[ $idle -eq 1 ]] && echo YES || echo NO)"

  if [[ $ANCESTRY_LIVE -eq 1 ]]; then
    port_abort "port $port holder pid $holder belongs to a LIVE claude/needle worker — do not kill; wait for it to finish or run the suite with MIROIR_TEST_SKIP_DOCKER=1 instead"
  fi

  if [[ $CHAIN_USER_MGR -eq 1 && -n "$min_live" && "$holder_start" -gt 0 \
        && "$holder_start" -lt "$min_live" && $idle -eq 1 ]]; then
    log "  verdict: OWNERLESS (orphaned under systemd --user, predates every live session, 30+ min idle)"
    if [[ $CHECK_ONLY -eq 1 ]]; then
      port_abort "port $port: --check mode — would SIGTERM ownerless holder pid $holder, taking no action"
    fi
    log "  SIGTERM $holder ..."
    kill -TERM "$holder"
    local i
    for i in $(seq 1 $((TERM_GRACE_SECS * 2))); do
      port_holder_pid "$port" >/dev/null 2>&1 || { log "  port $port released"; return 0; }
      sleep 0.5
    done
    port_abort "port $port still held by pid $holder after SIGTERM + ${TERM_GRACE_SECS}s — not escalating to SIGKILL (only SIGTERM is sanctioned); aborting"
  fi

  port_abort "port $port holder pid $holder: orphan-shape checks incomplete (chain-user-mgr=$CHAIN_USER_MGR predates-live-sessions=$([[ -n "$min_live" && "$holder_start" -gt 0 && "$holder_start" -lt "$min_live" ]] && echo 1 || echo 0) idle=$idle) — treating as owned; aborting without killing"
}

# --- Port pre-flight ---------------------------------------------------------
preflight_ports() {
  # Coarse check first (the runbook one-liner, colons anchored so e.g. :19090
  # cannot false-positive the :9090 pattern):
  if ! ss -ltnp 2>/dev/null | grep -E ":(17770|9090)[[:space:]]" >/dev/null; then
    log "pre-flight: ports $CLIENT_PORT and $METRICS_PORT are free"
    return 0
  fi
  log "pre-flight: ss reports a listener on 17770/9090 — classifying holder(s)"
  local held=0 port
  for port in "$CLIENT_PORT" "$METRICS_PORT"; do
    if port_holder_pid "$port" >/dev/null 2>&1; then
      resolve_holder "$port" || held=1     # returns 0 only when free/released
    fi
  done
  [[ $held -eq 1 ]] && port_abort "ports did not clear during pre-flight"
  log "pre-flight: ports cleared"
}

# --- Topology ----------------------------------------------------------------
resolve_proxy_bin() {
  if [[ -n "${MIROIR_PROXY_BIN:-}" ]]; then
    [[ -x "$MIROIR_PROXY_BIN" ]] || die "MIROIR_PROXY_BIN=$MIROIR_PROXY_BIN is not executable"
    printf '%s\n' "$MIROIR_PROXY_BIN"; return 0
  fi
  local candidate
  for candidate in "$REPO_DIR/target/debug/miroir-proxy" "$REPO_DIR/target/release/miroir-proxy"; do
    [[ -x "$candidate" ]] && { printf '%s\n' "$candidate"; return 0; }
  done
  log "no prebuilt miroir-proxy under $REPO_DIR/target — building (local, cgroup-limited)"
  (cd "$REPO_DIR" && cargo build -p miroir-proxy --bin miroir-proxy) >&2 \
    || die "cargo build -p miroir-proxy failed"
  [[ -x "$REPO_DIR/target/debug/miroir-proxy" ]] \
    || die "build produced no $REPO_DIR/target/debug/miroir-proxy"
  printf '%s\n' "$REPO_DIR/target/debug/miroir-proxy"
}

wait_meili_ready() {
  local url="http://127.0.0.1:$1/health" body i
  for i in $(seq 1 120); do   # 60 s: node answers /health in ~1 s when healthy
    body="$(curl -s -m 2 "$url")" && [[ "$body" == "$HEALTH_BODY" ]] && return 0
    sleep 0.5
  done
  log "meilisearch /health tail: $(curl -isS -m 2 "$url" 2>&1 | tail -n 3 | tr '\n' ' ')"
  docker logs --tail 20 "$MEILI_NAME" >&2 2>/dev/null
  die "meilisearch node did not report $HEALTH_BODY within 60 s"
}

# Render the proxy's miroir.yaml — byte-for-byte the shape proxy_config_yaml()
# emits for one node (verified against the suite's own generated file).
write_proxy_config() {
  local node_url="$1" task_db="$2"
  cat > "$CFG_DIR/miroir.yaml" <<EOF
master_key: $PROXY_MASTER_KEY
node_master_key: $NODE_MASTER_KEY
shards: 16
replication_factor: 1
replica_groups: 1
nodes:
  - id: node-0
    address: $node_url
    replica_group: 0
server:
  bind: 127.0.0.1
  port: $CLIENT_PORT
health:
  interval_ms: 200
  timeout_ms: 1000
task_store:
  backend: sqlite
  path: $task_db
result_cache:
  enabled: true
  ttl_ms: 500
  max_size: 1000
cdc:
  buffer:
    overflow: drop
search_ui:
  enabled: false
EOF
}

wait_proxy_ready() {
  local url="http://127.0.0.1:$CLIENT_PORT/health" body i
  for i in $(seq 1 $((READY_TIMEOUT_SECS * 2))); do
    if body="$(curl -s -m 2 "$url")"; then
      # The suite's rule: a 2xx with any body other than the exact health JSON
      # means a foreign process is on the port — bail instead of burning the
      # deadline. (Pre-flight already ensured the port was free, so any
      # answer at all should be ours; the exact-body guard is the backstop.)
      if [[ -n "$body" && "$body" != "$HEALTH_BODY" ]]; then
        die "GET /health returned an unexpected body '$body' — something other than this run's proxy is bound to $CLIENT_PORT"
      fi
      [[ "$body" == "$HEALTH_BODY" ]] && return 0
    fi
    kill -0 "$PROXY_PID" 2>/dev/null || { log "ABORT: proxy pid $PROXY_PID exited during readiness wait; tails follow"; proxy_log_tails; exit 1; }
    sleep 0.5
  done
  log "ABORT: proxy not ready within ${READY_TIMEOUT_SECS}s; tails follow"
  proxy_log_tails
  exit 1
}

proxy_log_tails() {
  local f
  for f in "$CFG_DIR/proxy.stdout.log" "$CFG_DIR/proxy.stderr.log"; do
    printf -- '--- %s (last 50 lines) ---\n' "$f" >&2
    tail -n 50 "$f" 2>/dev/null >&2
  done
}

# Assert the listener on the client port is OUR pid — the defense the suite's
# wait_for_ready lacks: a squatter that is itself a miroir-proxy would pass
# the exact-body health check, but it cannot pass the pid check.
assert_our_listener() {
  local lpid
  lpid="$(port_holder_pid "$CLIENT_PORT")" || die "no listener on $CLIENT_PORT despite successful /health"
  [[ "$lpid" == "$PROXY_PID" ]] || die "port $CLIENT_PORT is held by pid $lpid, not this run's proxy (pid $PROXY_PID)"
}

# --- Main --------------------------------------------------------------------
main() {
  require_tools

  # Default docker.sock is the sanctioned runtime on this box; an inherited
  # DOCKER_HOST (podman, TCP daemon) must not silently redirect the node.
  unset DOCKER_HOST
  [[ -S /var/run/docker.sock ]] || die "/var/run/docker.sock does not exist"
  docker info >/dev/null 2>&1 || die "docker daemon unreachable via /var/run/docker.sock"

  preflight_ports
  if [[ $CHECK_ONLY -eq 1 ]]; then
    log "--check: pre-flight passed, ports free; a real run would now spawn the topology"
    exit 0
  fi

  local proxy_bin
  proxy_bin="$(resolve_proxy_bin)"

  mkdir -p "$LOG_DIR"
  LOG_FILE="$LOG_DIR/capture-$RUN_TS.log"
  CFG_DIR="$(mktemp -d /tmp/p13_13-capture.XXXXXX)"

  # 1. Meilisearch node: env-only credentials (MEILI_MASTER_KEY — the image
  #    ENTRYPOINT is tini; CLI-arg form dies with exit 127), random host port
  #    via -P like testcontainers does.
  log "starting meilisearch ($MEILI_IMAGE) as $MEILI_NAME"
  MEILI_CID="$(docker run -d --rm --name "$MEILI_NAME" -P \
                -e MEILI_MASTER_KEY="$NODE_MASTER_KEY" "$MEILI_IMAGE")" \
    || { docker rm -f "$MEILI_NAME" >/dev/null 2>&1
         die "docker run $MEILI_IMAGE failed"; }
  local meili_port
  meili_port="$(docker port "$MEILI_NAME" "$MEILI_INTERNAL_PORT/tcp" | awk -F: '{print $NF}' | head -n 1)"
  [[ -n "$meili_port" ]] || die "could not resolve host port for $MEILI_INTERNAL_PORT/tcp on $MEILI_NAME"
  wait_meili_ready "$meili_port"
  log "meilisearch ready on 127.0.0.1:$meili_port"

  # 2. Proxy: cwd = its own config dir (MiroirConfig::load resolves relative to
  #    it), logs to files there — never pipes (nothing drains them; a full pipe
  #    buffer would stall the proxy mid-capture).
  write_proxy_config "http://127.0.0.1:$meili_port" "$CFG_DIR/miroir-tasks.db"
  log "spawning proxy: $proxy_bin (client :$CLIENT_PORT, metrics :$METRICS_PORT)"
  ( cd "$CFG_DIR" && exec "$proxy_bin" >proxy.stdout.log 2>proxy.stderr.log ) &
  PROXY_PID=$!
  wait_proxy_ready
  assert_our_listener
  log "proxy ready (pid $PROXY_PID)"

  # 3. Captures — verbatim status line + headers + body, teed to the log.
  {
    echo "=== p13_13 first-search capture $(date -u '+%Y-%m-%dT%H:%M:%SZ') ==="
    echo "proxy: pid $PROXY_PID  $proxy_bin"
    echo "ports: client $CLIENT_PORT, metrics $METRICS_PORT"
    echo "node:  $MEILI_IMAGE cid=$MEILI_CID host port $meili_port (key via MEILI_MASTER_KEY env only)"
    echo "auth:  Authorization: Bearer $PROXY_MASTER_KEY   body: $QUERY_BODY"
    local dirty
    dirty="$(git -C "$REPO_DIR" status --porcelain -- crates/miroir-proxy/src/routes/search.rs 2>/dev/null | head -n 1)"
    echo "git:   HEAD $(git -C "$REPO_DIR" rev-parse HEAD 2>/dev/null || echo unknown) search.rs: ${dirty:-<clean>}"
    echo "bin:   built $(stat -c %y "$proxy_bin" 2>/dev/null || echo unknown)"
  } >> "$LOG_FILE"

  local rc1=0 rc2=0
  echo "--- POST /indexes/products/search (path the p13_13 suite POSTs) ---" >> "$LOG_FILE"
  curl -isS -m 15 -X POST "http://127.0.0.1:$CLIENT_PORT/indexes/products/search" \
    -H "Authorization: Bearer $PROXY_MASTER_KEY" \
    -H "Content-Type: application/json" \
    -d "$QUERY_BODY" | tee -a "$LOG_FILE" || rc1=$?
  printf '\n' >> "$LOG_FILE"

  echo "--- POST /search/products (control: registered route, same process) ---" >> "$LOG_FILE"
  curl -isS -m 15 -X POST "http://127.0.0.1:$CLIENT_PORT/search/products" \
    -H "Authorization: Bearer $PROXY_MASTER_KEY" \
    -H "Content-Type: application/json" \
    -d "$QUERY_BODY" | tee -a "$LOG_FILE" || rc2=$?
  printf '\n' >> "$LOG_FILE"

  {
    echo "--- capture summary ---"
    echo "curl rc /indexes/products/search: $rc1 (0 = HTTP response received, any status)"
    echo "curl rc /search/products:         $rc2 (0 = HTTP response received, any status)"
    echo "end of capture $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  } >> "$LOG_FILE"

  log "responses teed verbatim to $LOG_FILE"
  if [[ $rc1 -ne 0 || $rc2 -ne 0 ]]; then
    die "at least one curl failed to get a response (rc1=$rc1 rc2=$rc2); log: $LOG_FILE"
  fi
  log "capture complete: $LOG_FILE"
}

main "$@"
