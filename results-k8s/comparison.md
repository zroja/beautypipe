# Apache Kafka 4.3.1 (KRaft) on Strimzi 1.2.0 vs Redpanda v25.2.3

Each cell: peak backlog (events) / drain time after the producer stops / max ready pods / pod-seconds (pod-failure rows add the time until the backlog was back under 1,000). Restarts are n/a for the Redpanda runs made before that counter existed. One run per scenario per broker, same VM, same code.

| Experiment | Apache Kafka 4.3.1 (KRaft) on Strimzi 1.2.0 | Redpanda v25.2.3 |
|---|---|---|
| Graceful pod deletion | 6,845 / 2 s / 1 / 48 / back under 1,000 after 5 s | 541 / 2 s / 1 / 48 / back under 1,000 after 2 s |
| Hard crash (SIGKILL) | 19,978 / 2 s / 1 / 46 / back under 1,000 after 11 s | 20,083 / 2 s / 1 / 46 / back under 1,000 after 11 s |
| Per-event sink, 1 pod | 138,840 / 180 s / 1 / 242 | 134,322 / 164 s / 1 / 226 |
| Per-event sink, KEDA autoscaling | 42,261 / 10 s / 8 / 428 | 33,813 / 7 s / 8 / 398 |
| Batched sink, 1 pod | 556 / 2 s / 1 / 64 | 548 / 2 s / 1 / 62 |
| Postgres 2 CPU, 1 pod | 573 / 2 s / 1 / 48 | 551 / 2 s / 1 / 48 |
| Postgres 0.05 CPU, 1 pod | 93,982 / 54 s / 1 / 100 | 96,498 / 56 s / 1 / 102 |
| Postgres 0.05 CPU, KEDA autoscaling | 91,998 / 52 s / 6 / 364 | 94,998 / 55 s / 5 / 370 |
| v2 schema, v1-only consumer, fail on bad event | 30,200 / 45 s / 1 / 24 | 30,065 / 45 s / 1 / 16 |
| v2 schema, v1-only consumer, dead-letter topic, then fix and redrive | 29,999 / 25 s / 1 / 72 | 30,001 / 19 s / 1 / 64 |
| v2 schema, consumer understands v1 and v2 | 219 / 2 s / 1 / 48 | 222 / 2 s / 1 / 48 |
| 0.2% malformed events, fail on bad event | 42,679 / 45 s / 1 / 14 | 43,195 / 45 s / 1 / 2 |
| 0.2% malformed events, fail on bad event, KEDA autoscaling | 42,063 / 45 s / 8 / 216 | 43,168 / 45 s / 1 / 12 |
| 0.5% malformed events, log and skip | 448 / 2 s / 1 / 48 | 425 / 2 s / 1 / 48 |
| 0.5% malformed events, dead-letter topic | 453 / 2 s / 1 / 48 | 438 / 2 s / 1 / 48 |

## Outcomes

| Experiment | Apache Kafka 4.3.1 (KRaft) on Strimzi 1.2.0 | Redpanda v25.2.3 |
|---|---|---|
| Graceful pod deletion | 89,990 of 89,990 stored; 0 restarts; 0 mismatches | 89,994 of 89,994 stored; n/a restarts; 0 mismatches |
| Hard crash (SIGKILL) | 89,995 of 89,995 stored; 1 restarts; 0 mismatches | 89,996 of 89,996 stored; n/a restarts; 0 mismatches |
| Per-event sink, 1 pod | 179,986 of 179,986 stored; 0 restarts; 0 mismatches | 179,992 of 179,992 stored; n/a restarts; 0 mismatches |
| Per-event sink, KEDA autoscaling | 179,992 of 179,992 stored; 0 restarts; 0 mismatches | 179,994 of 179,994 stored; n/a restarts; 0 mismatches |
| Batched sink, 1 pod | 179,990 of 179,990 stored; 0 restarts; 0 mismatches | 179,984 of 179,984 stored; n/a restarts; 0 mismatches |
| Postgres 2 CPU, 1 pod | 179,985 of 179,985 stored; 0 restarts; 0 mismatches | 179,995 of 179,995 stored; n/a restarts; 0 mismatches |
| Postgres 0.05 CPU, 1 pod | 179,982 of 179,982 stored; 0 restarts; 0 mismatches | 179,998 of 179,998 stored; n/a restarts; 0 mismatches |
| Postgres 0.05 CPU, KEDA autoscaling | 179,998 of 179,998 stored; 15 restarts; 0 mismatches | 179,995 of 179,995 stored; n/a restarts; 0 mismatches |
| v2 schema, v1-only consumer, fail on bad event | 14,799 of 44,999 stored (did not drain); 3 restarts; 0 mismatches | 14,930 of 44,995 stored (did not drain); 3 restarts; 0 mismatches |
| v2 schema, v1-only consumer, dead-letter topic, then fix and redrive | 44,997 of 44,997 stored, 29,999 dead-lettered; 0 restarts; 0 mismatches | 44,998 of 44,998 stored, 30,001 dead-lettered; 0 restarts; 0 mismatches |
| v2 schema, consumer understands v1 and v2 | 44,999 of 44,999 stored; 0 restarts; 0 mismatches | 44,998 of 44,998 stored; 0 restarts; 0 mismatches |
| 0.2% malformed events, fail on bad event | 2,316 of 44,995 stored (did not drain); 4 restarts; 0 mismatches | 1,800 of 44,995 stored (did not drain); 4 restarts; 0 mismatches |
| 0.2% malformed events, fail on bad event, KEDA autoscaling | 3,435 of 44,998 stored (did not drain); 24 restarts; 0 mismatches | 2,330 of 44,998 stored (did not drain); 26 restarts; 0 mismatches |
| 0.5% malformed events, log and skip | 44,760 of 44,997 stored; 0 restarts; 0 mismatches | 44,762 of 44,999 stored; 0 restarts; 0 mismatches |
| 0.5% malformed events, dead-letter topic | 44,761 of 44,998 stored, 237 dead-lettered; 0 restarts; 0 mismatches | 44,761 of 44,998 stored, 237 dead-lettered; 0 restarts; 0 mismatches |
