#!/usr/bin/env bash
# Swaps Redpanda for Apache Kafka managed by the Strimzi operator in the cluster created by k8s-up.sh,
# so the same experiments can run on a broker that is not a vendor product:
#
#   scripts/k8s-up.sh && scripts/k8s-strimzi-up.sh
#   python -m beautypipe.k8s_bench --broker strimzi      # -> results-k8s-strimzi/
set -euo pipefail

cd "$(dirname "$0")/.."
CLUSTER=beautypipe
STRIMZI_VERSION=1.2.0
KAFKA_VERSION=4.3.1

load_image() {
  local image="$1" archive
  docker pull -q "$image" >/dev/null
  archive="$(mktemp -t kind-image-XXXXXX.tar)"
  docker save --platform linux/amd64 "$image" -o "$archive"
  kind load image-archive "$archive" --name "$CLUSTER"
  rm -f "$archive"
}

# One broker at a time: both claim the host port 31092 and the VM has little memory to spare.
kubectl delete deploy/redpanda svc/redpanda svc/redpanda-external --ignore-not-found
kubectl delete scaledobject/consumer --ignore-not-found
kubectl scale deploy/consumer --replicas=0

load_image "quay.io/strimzi/operator:${STRIMZI_VERSION}"
load_image "quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}"

helm repo add strimzi https://strimzi.io/charts/ >/dev/null 2>&1 || true
helm repo update >/dev/null
helm upgrade --install strimzi strimzi/strimzi-kafka-operator --version "$STRIMZI_VERSION" \
  --namespace strimzi --create-namespace --set watchNamespaces="{default}" \
  --set image.imagePullPolicy=IfNotPresent --wait --timeout 240s

kubectl apply -f k8s/strimzi-kafka.yaml
kubectl wait kafka/beauty --for=condition=Ready --timeout=300s
echo "Kafka ready. Run: python -m beautypipe.k8s_bench --broker strimzi"
