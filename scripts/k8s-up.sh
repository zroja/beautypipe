#!/usr/bin/env bash
# Creates the kind cluster used by the Kubernetes experiments and deploys everything into it.
# Requires: docker, kind, kubectl, helm.
#
#   scripts/k8s-up.sh                    # Apache Kafka 4.3.1 managed by the Strimzi operator (default)
#   BROKER=redpanda scripts/k8s-up.sh    # Redpanda instead; run the experiments with --broker redpanda
#
# Only one broker runs at a time: both claim host port 31092 and the VM has little memory to spare.
set -euo pipefail

cd "$(dirname "$0")/.."
CLUSTER=beautypipe
KEDA_VERSION=2.21.0
REDPANDA_IMAGE=docker.redpanda.com/redpandadata/redpanda:v25.2.3
STRIMZI_VERSION=1.2.0
KAFKA_VERSION=4.3.1
BROKER="${BROKER:-strimzi}"

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --config k8s/kind-config.yaml --wait 180s

# Images are pulled on the host and loaded into the node. `docker save` + `image-archive` works with
# both the classic and the containerd image store (plain `kind load docker-image` fails with the latter).
load_image() {
  local image="$1" archive
  docker pull -q "$image" >/dev/null
  archive="$(mktemp -t kind-image-XXXXXX.tar)"
  docker save --platform linux/amd64 "$image" -o "$archive"
  kind load image-archive "$archive" --name "$CLUSTER"
  rm -f "$archive" || true
}

docker build -q -t beautypipe:dev .
load_image beautypipe:dev 2>/dev/null || kind load docker-image beautypipe:dev --name "$CLUSTER"
load_image postgres:16
if [ "$BROKER" = redpanda ]; then
  load_image "$REDPANDA_IMAGE"
else
  load_image "quay.io/strimzi/operator:${STRIMZI_VERSION}"
  load_image "quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}"
fi
for image in keda keda-admission-webhooks keda-metrics-apiserver; do
  load_image "ghcr.io/kedacore/${image}:${KEDA_VERSION}"
done

# Some kernels (for example minimal cloud VMs) lack the netfilter `statistic` module that kube-proxy
# needs for Services with more than one backend, which breaks every Service. One DNS replica avoids it.
kubectl -n kube-system scale deploy/coredns --replicas=1

helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install keda kedacore/keda --version "$KEDA_VERSION" --namespace keda --create-namespace \
  --set image.pullPolicy=IfNotPresent --wait --timeout 240s

kubectl delete scaledobject/consumer --ignore-not-found
kubectl apply -f k8s/postgres.yaml -f k8s/consumer.yaml
kubectl scale deploy/consumer --replicas=0

if [ "$BROKER" = redpanda ]; then
  kubectl delete kafka/beauty kafkanodepool/dual --ignore-not-found
  kubectl apply -f k8s/redpanda.yaml
  kubectl rollout status deploy/redpanda --timeout=240s
else
  kubectl delete deploy/redpanda svc/redpanda svc/redpanda-external --ignore-not-found
  helm repo add strimzi https://strimzi.io/charts/ >/dev/null 2>&1 || true
  helm repo update >/dev/null
  helm upgrade --install strimzi strimzi/strimzi-kafka-operator --version "$STRIMZI_VERSION" \
    --namespace strimzi --create-namespace --set watchNamespaces="{default}" \
    --set image.imagePullPolicy=IfNotPresent --wait --timeout 240s
  kubectl apply -f k8s/strimzi-kafka.yaml
  kubectl wait kafka/beauty --for=condition=Ready --timeout=300s
fi
kubectl rollout status deploy/postgres --timeout=240s

kubectl delete job catalog-seed --ignore-not-found
kubectl apply -f k8s/batch.yaml
kubectl wait --for=condition=complete job/catalog-seed --timeout=180s

echo
if [ "$BROKER" = redpanda ]; then
  echo "Cluster ready (Redpanda). Run: python -m beautypipe.k8s_bench --broker redpanda"
else
  echo "Cluster ready (Apache Kafka on Strimzi). Run: python -m beautypipe.k8s_bench"
fi
