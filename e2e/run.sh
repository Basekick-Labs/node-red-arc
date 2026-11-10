#!/usr/bin/env bash
# End-to-end run on Docker (Rancher Desktop works):
#   - Arc 26.09.3 in a container (no refusal headers: at-most-once path)
#   - Arc main as a native binary on the host when ARC_MAIN_BIN is set
#     (refusal headers, partial writes)
#   - Node-RED 4.1.8 and 5.0.9 containers with this package installed
# Each Node-RED instance writes to every Arc target through real flows.
set -euo pipefail
cd "$(dirname "$0")/.."

TOKEN="nodered-e2e-token-0123456789abcdef0123456789"
NET=nrarc-e2e
WORK="$(pwd)/e2e/.data"
ARC26_PORT=18000
ARCMAIN_PORT=18001
NR4_PORT=18804
NR5_PORT=18805

cleanup() {
  docker rm -f nrarc-arc26 nrarc-nr4 nrarc-nr5 >/dev/null 2>&1 || true
  [ -f "$WORK/arc-main.pid" ] && kill "$(cat "$WORK/arc-main.pid")" 2>/dev/null || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
rm -rf "$WORK" && mkdir -p "$WORK"/{nr4,nr5,arc26,arcmain}

wait_http() { # url, seconds
  for _ in $(seq 1 "$2"); do curl -sf -o /dev/null "$1" && return 0; sleep 1; done
  echo "timeout waiting for $1" >&2; return 1
}

docker network create "$NET" >/dev/null

# --- Arc 26.09.3 ---
cat > "$WORK/arc26-start.sh" <<EOF
docker start nrarc-arc26 >/dev/null
EOF
docker run -d --name nrarc-arc26 --network "$NET" -p $ARC26_PORT:8000 \
  -e ARC_AUTH_BOOTSTRAP_TOKEN="$TOKEN" -e ARC_COMPACTION_ENABLED=false \
  -v "$WORK/arc26:/app/data" basekicklabs/arc:26.09.3 >/dev/null
wait_http "http://localhost:$ARC26_PORT/health" 60

TARGETS='[{"name":"a26","hostUrl":"http://localhost:'$ARC26_PORT'","nrArcUrl":"http://nrarc-arc26:8000","db":"nodered_e2e","stop":"docker stop -t 2 nrarc-arc26","start":"docker start nrarc-arc26"}'

# --- Arc main (native) ---
if [ -n "${ARC_MAIN_BIN:-}" ]; then
  cat > "$WORK/arc-main.sh" <<EOF
#!/usr/bin/env bash
cd "$WORK/arcmain"
case "\$1" in
  start)
    ARC_SERVER_PORT=$ARCMAIN_PORT ARC_AUTH_BOOTSTRAP_TOKEN="$TOKEN" ARC_COMPACTION_ENABLED=false \
    ARC_STORAGE_LOCAL_PATH="$WORK/arcmain/data/arc" ARC_AUTH_DB_PATH="$WORK/arcmain/data/arc.db" \
      nohup "$ARC_MAIN_BIN" > "$WORK/arcmain/arc.log" 2>&1 &
    echo \$! > "$WORK/arc-main.pid"
    for _ in \$(seq 1 60); do curl -sf -o /dev/null http://localhost:$ARCMAIN_PORT/health && exit 0; sleep 0.5; done
    exit 1;;
  stop)
    kill "\$(cat "$WORK/arc-main.pid")"; sleep 1;;
esac
EOF
  chmod +x "$WORK/arc-main.sh"
  "$WORK/arc-main.sh" start
  TARGETS="$TARGETS"',{"name":"amain","hostUrl":"http://localhost:'$ARCMAIN_PORT'","nrArcUrl":"http://host.docker.internal:'$ARCMAIN_PORT'","db":"nodered_e2e","stop":"'$WORK'/arc-main.sh stop","start":"'$WORK'/arc-main.sh start"}'
fi
TARGETS="$TARGETS]"

# --- Node-RED instances with the packed package ---
npm pack --silent --pack-destination "$WORK" >/dev/null
PKG=$(ls "$WORK"/basekick-labs-node-red-arc-*.tgz)
start_nr() { # name image port dir
  cp "$PKG" "$WORK/$4/pkg.tgz"
  chmod -R a+rwX "$WORK/$4"
  docker run -d --name "$1" --network "$NET" -p "$3:1880" -v "$WORK/$4:/data" "$2" >/dev/null
  wait_http "http://localhost:$3/" 90
  docker exec -w /data "$1" npm install --no-audit --no-fund --silent ./pkg.tgz >/dev/null
  docker restart "$1" >/dev/null
  wait_http "http://localhost:$3/" 90
  curl -sf "http://localhost:$3/nodes" -H 'Accept: application/json' | grep -q '"arc-write"' || { echo "arc nodes not loaded in $1" >&2; docker logs "$1" | tail -30; exit 1; }
}
start_nr nrarc-nr4 nodered/node-red:4.1.8-22 $NR4_PORT nr4
start_nr nrarc-nr5 nodered/node-red:5.0.9 $NR5_PORT nr5

export E2E_TOKEN="$TOKEN"
status=0
# Each Node-RED gets its own database so the counts do not mix.
E2E_TARGETS="${TARGETS//nodered_e2e/nr4}" node e2e/run.js "http://localhost:$NR4_PORT" "node-red 4.1.8" || status=1
E2E_TARGETS="${TARGETS//nodered_e2e/nr5}" node e2e/run.js "http://localhost:$NR5_PORT" "node-red 5.0.9" || status=1

if [ $status -ne 0 ]; then
  echo "--- node-red 4 log ---"; docker logs nrarc-nr4 2>&1 | grep -iE "arc|error|warn" | tail -30
  echo "--- node-red 5 log ---"; docker logs nrarc-nr5 2>&1 | grep -iE "arc|error|warn" | tail -30
fi
exit $status
