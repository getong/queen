#!/usr/bin/env bash
# run.sh — runs wl (main.go) as one pod inside the STAGE cluster, next to queen-mq-v2, so the latency it prints is
# the cluster's and not a port-forward's. The context is pinned to stage: the default context on this Mac is prod.
#
#   ./run.sh start [wl flags]   build + run (replaces a running pod). Defaults: 5 queues, 10,000 partitions in
#                               total, 1,000 msg/s, 10 minutes. Example: ./run.sh start -rate 2000 -duration 5m
#   ./run.sh prove              the 2.2.0 memory rule, about 58 minutes: 1,000 msg/s for 30 minutes into queues with
#                               row windows of 900, 1,200 and 1,500 s, 26 minutes of watching the rows leave, then one
#                               queue is read for the first time. Every line sets the rows in each broker's RAM
#                               against the model (see README.md)
#   ./run.sh logs               follow the output (one line every 10 s, the result table at the end)
#   ./run.sh top                CPU and memory of the brokers and the loader, now
#   ./run.sh mem                each broker now: rows in RAM (one per push), anonymous memory, page cache, total
#   ./run.sh stop               end the run now; wl drains and prints its result table before it exits
#   ./run.sh clean              delete the queues the workload created (wl-0 .. wl-4), then the pod and ConfigMap
#
# The pod builds main.go from a ConfigMap with the Go toolchain image (standard library only, no network), so
# nothing is pushed to a registry. The build takes about a minute before the first line.
set -euo pipefail

CTX=gke_smartness-stage_europe-west1_k8s-03-stage
NS=queen
POD=queen-workload
CM=queen-workload-src
URL=http://queen-mq-v2.queen.svc.cluster.local:6632
# each broker by itself, for its metrics (the rows in RAM)
H=queen-mq-v2-headless.queen.svc.cluster.local:6632
BROKERS=http://queen-mq-v2-0.$H,http://queen-mq-v2-1.$H,http://queen-mq-v2-2.$H
IMAGE=golang:1.26-alpine
DIR=$(cd "$(dirname "$0")" && pwd)

k() { kubectl --context "$CTX" -n "$NS" "$@"; }

start() {
  k create configmap "$CM" --from-file=main.go="$DIR/main.go" --from-file=go.mod="$DIR/go.mod" \
    --dry-run=client -o yaml | k apply -f - >/dev/null
  k delete pod "$POD" --ignore-not-found --wait=true >/dev/null
  local args=""
  for a in "$@"; do
    a=${a//\\/\\\\}
    args+="        - \"${a//\"/\\\"}\""$'\n'
  done
  k apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels:
    app: $POD
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 90
  # never on a broker's node: at 10,000 msg/s the loader takes 2.5-3 cores, and next to a broker it filled the node
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels: { run: queen-mq-v2 }
          topologyKey: kubernetes.io/hostname
  securityContext:
    runAsNonRoot: true
    runAsUser: 10001
    runAsGroup: 10001
    fsGroup: 10001
  containers:
    - name: wl
      image: $IMAGE
      securityContext:
        allowPrivilegeEscalation: false
        capabilities:
          drop: ["ALL"]
      env:
        - { name: HOME, value: /work }
        - { name: GOCACHE, value: /work/.cache }
        - { name: GOPATH, value: /work/go }
        - { name: GOFLAGS, value: -mod=mod }
        - { name: GOPROXY, value: "off" }
        - { name: GOTOOLCHAIN, value: local }
        - { name: CGO_ENABLED, value: "0" }
      command:
        - sh
        - -c
        - cp /src/main.go /src/go.mod /work/ && cd /work && go build -o /work/wl . && exec /work/wl -url $URL -brokers $BROKERS "\$@"
        - wl
      args:
$args
      resources:
        requests: { cpu: 500m, memory: 256Mi }
        limits: { memory: 1Gi }
      volumeMounts:
        - { name: src, mountPath: /src }
        - { name: work, mountPath: /work }
  volumes:
    - name: src
      configMap: { name: $CM }
    - name: work
      emptyDir: {}
EOF
  echo "pod $POD started in $NS on stage; follow it with: $0 logs"
}

case "${1:-}" in
  start) shift; start "$@" ;;
  prove)
    # floor 900 s on stage. wl-0, wl-1: dedup window under the floor -> 900 s; wl-2: the same with a completed
    # retention of 1,800 s, which keeps the messages on disk and (from 2.2.0-beta.2) no row -> 900 s; wl-3: dedup
    # window 1,200 s; wl-4: dedup window 1,500 s, and no consumer until every row has left
    start -rate 1000 -duration 30m -linger 26m -warm=false -floor 900 \
      -dedup-window 0,0,0,1200,1500 -completed-retention 60,60,1800,60,60 \
      -late-queues 1 -retention 7200 -stop-rss-mb 1800
    ;;
  mem)
    for i in 0 1 2; do
      k exec "queen-mq-v2-$i" -- sh -c 'curl -s -m 10 localhost:6632/metrics/prometheus | grep "^queen_raft_store_ram_rows{ks=\"txns\",kind=\"live\"}"; grep -E "^(anon|file) " /sys/fs/cgroup/memory.stat; cat /sys/fs/cgroup/memory.current' |
        awk -v p="queen-mq-v2-$i" 'NR==1{r=$2} $1=="anon"{a=$2} $1=="file"{f=$2} END{printf "%s  rows %d  anon %.0f MB  page cache %.0f MB  total %.0f MB\n", p, r, a/1048576, f/1048576, $1/1048576}'
    done
    ;;
  logs) k logs -f "$POD" ;;
  top) k top pod | grep -E "NAME|queen-mq-v2|$POD" ;;
  stop) k exec "$POD" -- kill -TERM 1 && echo "stopping; the result table is in: $0 logs" ;;
  clean)
    start -clean
    k wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$POD" --timeout=300s >/dev/null
    k logs "$POD"
    k delete pod "$POD" >/dev/null
    k delete configmap "$CM" >/dev/null
    ;;
  *) sed -n '2,19p' "$0"; exit 2 ;;
esac
