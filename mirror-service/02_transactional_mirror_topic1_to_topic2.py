#!/usr/bin/env python3
"""
+----------+              +-------------------------+              +------------------------+

|          |              |       Transaction       |              |  __transaction_state   |
| Producer |              |       Coordinator       |              |         Topic          |
+----+-----+              +------------+------------+              +-----------+------------+

     |                                 |                                       |
     | (1) InitProducerId              |                                       |
     |-------------------------------->|                                       |
     |                                 | (2) Log: Empty / Ongoing              |
     |                                 |-------------------------------------->|
     |                                 |                                       |
     | (3) AddPartitionsToTxnRequest   |                                       |
     |-------------------------------->|                                       |
     |                                 | (4) Log: Partitions Added             |
     |                                 |-------------------------------------->|
     |                                 |                                       |
     | (5) Produce Request             |                                       |
     |---------------------------------+------------------------------------> [ Kafka Topic Partitions ]

     |                                 |                                       |
     | (6) EndTxnRequest (Commit/Abort)|                                       |
     |-------------------------------->|                                       |
     |                                 | (7) Log: Prepare Commit/Abort         |
     |                                 |-------------------------------------->|
     |                                 |                                       |
     |                                 | (8) Write Control Markers             |
     |                                 |------------------------------------> [ Kafka Topic Partitions ]
     |                                 |                                       |
     |                                 | (9) Write Offsets Control Marker      |
     |                                 |------------------------------------> [ __consumer_offsets ]
     |                                 |                                       |
     |                                 | (10) Log: Complete Commit/Abort       |
     |                                 |-------------------------------------->|
     v                                 v                                       v

In a typical "consume-transform-produce" loop, the process works as follows:Poll Records:
The consumer pulls records from an input topic.Begin Transaction:
The producer initiates a transaction via producer.beginTransaction().
Produce Result: The application processes the data and the producer sends the new records to output topics using producer.send().
 These records are written to the broker but remain "uncommitted" and invisible to downstream consumers using read_committed mode.
 Send Offsets:
 Instead of the consumer committing its own offsets, the producer sends them to the transaction coordinator using producer.sendOffsetsToTransaction().
 Commit Transaction:
 The producer calls producer.commitTransaction().
 This atomically commits both the produced records and the consumer's offsets.








=============================================================================
 STEP 2 -- Transactional Kafka Mirror  :  Topic 1  ->  Topic 2
=============================================================================
 Reads Avro records from:
   Source Topic : kolkata-locations-raw    (Topic 1)
 Writes them transactionally to:
   Sink Topic   : kolkata-locations-mirror (Topic 2)

 Running inside Docker (KRaft cluster)
 -------------------------------------------------------------------------
  Bootstrap  : kafka-1:29092,kafka-2:29093,kafka-3:29094  (INTERNAL)
  Schema Reg : http://schema-registry-1:8081

  All values are overridable via environment variables:
    BOOTSTRAP_SERVERS     (default: kafka-1:29092,kafka-2:29093,kafka-3:29094)
    SCHEMA_REGISTRY_URL   (default: http://schema-registry-1:8081)
    INSTANCE_ID           (default: 0 -- set to 1/2/3 per container)
=============================================================================
"""

import io
import json
import logging
import os
import signal
import sys
import time
from typing import Dict, List

import requests
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringDeserializer,
    StringSerializer,
)

# -----------------------------------------------------------------------------
#  Logging  -- INSTANCE_ID is injected per container
# -----------------------------------------------------------------------------
INSTANCE_ID = os.environ.get("INSTANCE_ID", "0")

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s  %(levelname)-8s  [inst-{INSTANCE_ID}]  %(name)s  |  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")),
        logging.FileHandler(f"mirror_topic2_inst{INSTANCE_ID}.log", mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger("KolkataMirror")

# -----------------------------------------------------------------------------
#  Configuration -- all overridable via environment
# -----------------------------------------------------------------------------
BOOTSTRAP_SERVERS   = os.environ.get(
    "BOOTSTRAP_SERVERS",
    "kafka-1:29092,kafka-2:29093,kafka-3:29094",
)
SCHEMA_REGISTRY_URL = os.environ.get(
    "SCHEMA_REGISTRY_URL",
    "http://schema-registry-1:8081",
)

SOURCE_TOPIC      = "kolkata-locations-raw"
SINK_TOPIC        = "kolkata-locations-mirror"
CONSUMER_GROUP_ID = "kolkata-mirror-consumer-group"
TXN_ID_PREFIX     = "kolkata-mirror-raw"

TRANSACTION_BATCH_SIZE         = 10
TRANSACTION_COMMIT_INTERVAL_MS = 5_000

TXN_INIT_MAX_ATTEMPTS = 15
TXN_INIT_BACKOFF_S    = 3.0

# -----------------------------------------------------------------------------
#  Avro Schema  (same schema as Topic 1; mirrored verbatim)
# -----------------------------------------------------------------------------
LOCATION_SCHEMA_STR = json.dumps(
    {
        "type": "record",
        "name": "KolkataLocation",
        "namespace": "com.kolkata.locations",
        "doc": "A named location within Kolkata city",
        "fields": [
            {"name": "id",        "type": "int"},
            {"name": "name",      "type": "string"},
            {"name": "zone",      "type": "string"},
            {"name": "district",  "type": "string"},
            {"name": "pincode",   "type": "string"},
            {"name": "latitude",  "type": "double"},
            {"name": "longitude", "type": "double"},
            {"name": "landmark",  "type": ["null", "string"], "default": None},
            {"name": "ts_epoch",  "type": "long"},
        ],
    }
)

# -----------------------------------------------------------------------------
#  Graceful Shutdown
# -----------------------------------------------------------------------------
_running = True


def _handle_signal(sig, frame):  # noqa: ANN001
    global _running
    log.warning("[SIGNAL] Received signal %s -- initiating graceful shutdown ...", sig)
    _running = False


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# -----------------------------------------------------------------------------
#  Helper: build stable transactional.id from partition list
# -----------------------------------------------------------------------------
def _build_transactional_id(partitions: List[TopicPartition]) -> str:
    part_suffix = ".".join(
        f"p{tp.partition}"
        for tp in sorted(partitions, key=lambda x: x.partition)
    )
    return f"{TXN_ID_PREFIX}-{part_suffix}"


# -----------------------------------------------------------------------------
#  Helper: wait for Schema Registry
# -----------------------------------------------------------------------------
def _wait_for_schema_registry(url: str, max_attempts: int = 20) -> None:
    log.info("[INIT] Waiting for Schema Registry at %s ...", url)
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(f"{url}/subjects", timeout=5)
            resp.raise_for_status()
            log.info("[INIT] Schema Registry is UP  (attempt %d)", attempt)
            return
        except Exception as exc:
            log.warning("[INIT] SR not ready (attempt %d/%d): %s",
                        attempt, max_attempts, exc)
            time.sleep(5)
    log.error("[INIT] Schema Registry unreachable after %d attempts. Aborting.", max_attempts)
    sys.exit(1)


# -----------------------------------------------------------------------------
#  Build Transactional Producer  (single attempt)
# -----------------------------------------------------------------------------
def _build_txn_producer(
    sr_client: SchemaRegistryClient,
    transactional_id: str,
) -> tuple:
    log.info("[PRODUCER] Building Transactional Producer | txn.id='%s' ...",
             transactional_id)

    avro_serializer   = AvroSerializer(sr_client, LOCATION_SCHEMA_STR, lambda o, c: o)
    string_serializer = StringSerializer("utf_8")

    producer_conf = {
        "bootstrap.servers":      BOOTSTRAP_SERVERS,
        "transactional.id":       transactional_id,
        "enable.idempotence":     True,
        "acks":                   "all",
        "retries":                20,
        "retry.backoff.ms":       500,
        "compression.type":       "snappy",
        "linger.ms":              5,
        "batch.size":             32_768,
        "transaction.timeout.ms": 60_000,
    }

    producer = Producer(producer_conf)
    log.info("[PRODUCER] Calling init_transactions() ...")
    producer.init_transactions()
    log.info("[PRODUCER] Transactions initialised  txn.id='%s'", transactional_id)
    return producer, avro_serializer, string_serializer


# -----------------------------------------------------------------------------
#  Build Transactional Producer with retry  (FIX for BUG 2)
# -----------------------------------------------------------------------------
_RETRYABLE_TXN_INIT_CODES = frozenset([
    int(KafkaError.COORDINATOR_LOAD_IN_PROGRESS),
    int(KafkaError.COORDINATOR_NOT_AVAILABLE),
])


def _build_txn_producer_with_retry(
    sr_client: SchemaRegistryClient,
    transactional_id: str,
    max_attempts: int = TXN_INIT_MAX_ATTEMPTS,
    backoff_s: float  = TXN_INIT_BACKOFF_S,
) -> tuple:
    for attempt in range(1, max_attempts + 1):
        try:
            return _build_txn_producer(sr_client, transactional_id)
        except KafkaException as exc:
            kafka_err = exc.args[0]
            err_code  = int(kafka_err.code())
            if err_code in _RETRYABLE_TXN_INIT_CODES:
                log.warning(
                    "[PRODUCER] init_transactions attempt %d/%d failed "
                    "(code=%d: %s) -- retrying in %.1f s ...",
                    attempt, max_attempts, err_code, kafka_err.str(), backoff_s,
                )
                time.sleep(backoff_s)
            else:
                log.error(
                    "[PRODUCER] init_transactions non-retryable error "
                    "on attempt %d/%d: %s",
                    attempt, max_attempts, exc,
                )
                raise

    raise RuntimeError(
        f"Could not initialise transactions for txn.id='{transactional_id}' "
        f"after {max_attempts} attempts. "
        f"Check broker health and __transaction_state replication."
    )


# -----------------------------------------------------------------------------
#  Rebalance Listener  (producer creation deferred -- FIX for BUG 2)
# -----------------------------------------------------------------------------
class MirrorRebalanceListener:
    def __init__(self, sr_client: SchemaRegistryClient, state: dict):
        self._sr_client = sr_client
        self._state     = state

    def on_assign(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        assigned = [tp.partition for tp in partitions]
        log.info("[REBALANCE] on_assign  partitions=%s", assigned)

        if self._state.get("producer") is not None:
            log.warning("[REBALANCE] Tearing down previous producer ...")
            try:
                self._state["producer"].abort_transaction()
            except Exception as exc:
                log.warning("[REBALANCE] abort_transaction (old): %s", exc)
            self._state["producer"] = None
            self._state["txn_id"]   = None

        if not partitions:
            log.warning("[REBALANCE] No partitions assigned.")
            self._state["pending_partitions"] = None
            return

        # Defer producer build to main loop (no blocking RPCs on poll thread)
        self._state["pending_partitions"] = partitions
        log.info("[REBALANCE] Partitions queued for deferred producer build.")

    def on_revoke(self, consumer: Consumer, partitions: List[TopicPartition]) -> None:
        revoked = [tp.partition for tp in partitions]
        log.warning("[REBALANCE] on_revoke  partitions=%s", revoked)

        self._state["pending_partitions"] = None

        if self._state.get("producer") is not None:
            log.warning("[REBALANCE] Aborting in-flight transaction ...")
            try:
                self._state["producer"].abort_transaction()
            except Exception as exc:
                log.warning("[REBALANCE] abort_transaction: %s", exc)
            self._state["producer"] = None
            self._state["txn_id"]   = None

        log.warning("[REBALANCE] Producer torn down.")


# -----------------------------------------------------------------------------
#  Build Consumer
# -----------------------------------------------------------------------------
def _build_consumer(sr_client: SchemaRegistryClient) -> tuple:
    log.info("[CONSUMER] Building Kafka Consumer ...")

    avro_deserializer   = AvroDeserializer(sr_client, LOCATION_SCHEMA_STR)
    string_deserializer = StringDeserializer("utf_8")

    consumer_conf = {
        "bootstrap.servers":             BOOTSTRAP_SERVERS,
        "group.id":                      CONSUMER_GROUP_ID,
        "auto.offset.reset":             "earliest",
        "isolation.level":               "read_committed",
        "enable.auto.commit":            False,
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms":            30_000,
        "heartbeat.interval.ms":         3_000,
        "max.poll.interval.ms":          300_000,
    }

    consumer = Consumer(consumer_conf)
    log.info("[CONSUMER] Consumer created  group='%s'", CONSUMER_GROUP_ID)
    return consumer, avro_deserializer, string_deserializer


# -----------------------------------------------------------------------------
#  Offset helper  (FIX for BUG 1 -- no .topic_partition() on Message)
# -----------------------------------------------------------------------------
def _build_offsets_to_commit(batch_messages: list) -> List[TopicPartition]:
    consumer_positions: Dict[tuple, TopicPartition] = {}

    for msg_obj, _value, _key in batch_messages:
        topic       = msg_obj.topic()
        partition   = msg_obj.partition()
        next_offset = msg_obj.offset() + 1
        tp_key      = (topic, partition)

        if tp_key not in consumer_positions or \
                next_offset > consumer_positions[tp_key].offset:
            consumer_positions[tp_key] = TopicPartition(topic, partition, next_offset)
        log.info("Topic Partitions are %s", consumer_positions[tp_key])

    return list(consumer_positions.values())


# -----------------------------------------------------------------------------
#  Main
# -----------------------------------------------------------------------------
def main() -> None:
    log.info("=" * 70)
    log.info("  STEP 2 -- Transactional Kafka Mirror  [instance-%s]", INSTANCE_ID)
    log.info("  Source          : %s", SOURCE_TOPIC)
    log.info("  Sink            : %s", SINK_TOPIC)
    log.info("  Bootstrap       : %s", BOOTSTRAP_SERVERS)
    log.info("  Schema Registry : %s", SCHEMA_REGISTRY_URL)
    log.info("  Consumer Group  : %s", CONSUMER_GROUP_ID)
    log.info("  Txn ID format   : %s-p<N>.<M>...  (after rebalance)", TXN_ID_PREFIX)
    log.info("  Txn init retry  : max_attempts=%d  backoff=%.1f s",
             TXN_INIT_MAX_ATTEMPTS, TXN_INIT_BACKOFF_S)
    log.info("=" * 70)

    _wait_for_schema_registry(SCHEMA_REGISTRY_URL)

    log.info("[SR] Connecting to Schema Registry ...")
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    log.info("[SR] SchemaRegistryClient created")

    producer_state: dict = {
        "producer":           None,
        "avro_ser":           None,
        "str_ser":            None,
        "txn_id":             None,
        "pending_partitions": None,
    }

    consumer, avro_deser, str_deser = _build_consumer(sr_client)
    listener = MirrorRebalanceListener(sr_client, producer_state)

    log.info("[CONSUMER] Subscribing to '%s' ...", SOURCE_TOPIC)
    consumer.subscribe(
        [SOURCE_TOPIC],
        on_assign=listener.on_assign,
        on_revoke=listener.on_revoke,
    )
    log.info("[CONSUMER] Subscribed  (waiting for partition assignment ...)")

    total_consumed  = 0
    total_produced  = 0
    total_txn_ok    = 0
    total_txn_abort = 0

    log.info("[MIRROR] Starting main loop  batch_size=%d  commit_interval=%d ms",
             TRANSACTION_BATCH_SIZE, TRANSACTION_COMMIT_INTERVAL_MS)

    try:
        while _running:

            # ------------------------------------------------------------------
            #  Phase 1: deferred producer build (triggered by on_assign)
            # ------------------------------------------------------------------
            if producer_state["pending_partitions"] is not None:
                pending = producer_state["pending_partitions"]
                txn_id  = _build_transactional_id(pending)
                log.info(
                    "[MIRROR] Building producer  partitions=%s  txn.id='%s' ...",
                    [tp.partition for tp in pending], txn_id,
                )
                try:
                    producer, avro_ser, str_ser = _build_txn_producer_with_retry(
                        sr_client, txn_id
                    )
                    producer_state.update({
                        "producer":           producer,
                        "avro_ser":           avro_ser,
                        "str_ser":            str_ser,
                        "txn_id":             txn_id,
                        "pending_partitions": None,
                    })
                    log.info("[MIRROR] Producer ready  txn.id='%s'", txn_id)
                except RuntimeError as exc:
                    log.error("[MIRROR] %s  Shutting down.", exc)
                    break
                continue

            # ------------------------------------------------------------------
            #  Phase 2: wait for rebalance / on_assign
            # ------------------------------------------------------------------
            if producer_state["producer"] is None:
                log.debug("[MIRROR] Waiting for partition assignment ...")
                consumer.poll(timeout=1.0)
                continue

            # ------------------------------------------------------------------
            #  Phase 3: gather a batch
            # ------------------------------------------------------------------
            batch_messages = []
            batch_start_ms = int(time.time() * 1000)

            log.info(
                "[TXN]  Gathering batch (max=%d  window=%d ms  txn.id='%s') ...",
                TRANSACTION_BATCH_SIZE, TRANSACTION_COMMIT_INTERVAL_MS,
                producer_state["txn_id"],
            )

            while len(batch_messages) < TRANSACTION_BATCH_SIZE and _running:

                if producer_state["producer"] is None:
                    log.warning("[TXN]  Producer revoked mid-batch -- discarding")
                    batch_messages.clear()
                    break

                elapsed = int(time.time() * 1000) - batch_start_ms
                log.info(f"[COMMON] elapsed time is {elapsed} ms and Transaction_commit_interval is {TRANSACTION_COMMIT_INTERVAL_MS} ms")

                if elapsed >= TRANSACTION_COMMIT_INTERVAL_MS:
                    log.info("[TXN]  Commit interval elapsed (%d ms) -- flushing", elapsed)
                    break

                poll_timeout = max(0.1, (TRANSACTION_COMMIT_INTERVAL_MS - elapsed) / 1000)
                msg = consumer.poll(timeout=poll_timeout)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        log.info("[CONSUMER] EOF  partition=%d  offset=%d",
                                 msg.partition(), msg.offset())
                        continue
                    raise KafkaException(msg.error())

                key_str = str_deser(msg.key()) if msg.key() else None
                value   = avro_deser(
                    msg.value(),
                    SerializationContext(SOURCE_TOPIC, MessageField.VALUE),
                )

                log.info(
                    "[CONSUMER] partition=%d  offset=%d  key=%s  "
                    "id=%-3s  zone=%-8s  name=%s",
                    msg.partition(), msg.offset(), key_str,
                    value.get("id")   if value else "?",
                    value.get("zone") if value else "?",
                    value.get("name") if value else "?",
                )

                batch_messages.append((msg, value, key_str))
                total_consumed += 1

            if not batch_messages:
                if not _running:
                    break
                time.sleep(0.5)
                continue

            producer = producer_state["producer"]
            avro_ser = producer_state["avro_ser"]
            str_ser  = producer_state["str_ser"]
            txn_id   = producer_state["txn_id"]

            if producer is None:
                log.warning("[TXN]  Producer gone before begin -- skipping")
                continue

            # ------------------------------------------------------------------
            #  TRANSACTION
            # ------------------------------------------------------------------
            txn_label = (
                f"txn-{total_txn_ok + total_txn_abort + 1:05d}"
                f"  [{len(batch_messages)} records]"
                f"  txn.id='{txn_id}'"
            )
            log.info("[TXN]  --- BEGIN %s ---", txn_label)
            producer.begin_transaction()

            try:
                for msg_obj, value, key_str in batch_messages:
                    serialized_key   = str_ser(key_str) if key_str else None
                    serialized_value = avro_ser(
                        value,
                        SerializationContext(SINK_TOPIC, MessageField.VALUE),
                    )
                    log.info(
                        "[TXN]    -> %s  key=%s  id=%-3s  zone=%-8s  name=%s",
                        SINK_TOPIC, key_str,
                        value.get("id")   if value else "?",
                        value.get("zone") if value else "?",
                        value.get("name") if value else "?",
                    )
                    producer.produce(
                        topic=SINK_TOPIC,
                        key=serialized_key,
                        value=serialized_value,
                    )
                    total_produced += 1

                offsets_to_commit = _build_offsets_to_commit(batch_messages)
                log.info("[TXN]    Offsets -> %s",
                         [(o.topic, o.partition, o.offset) for o in offsets_to_commit])

                producer.send_offsets_to_transaction(
                    offsets_to_commit,
                    consumer.consumer_group_metadata(),
                )
                producer.commit_transaction()
                total_txn_ok += 1
                log.info("[TXN]  --- COMMITTED %s ---", txn_label)

            except KafkaException as kafka_exc:
                log.error("[TXN]  KafkaException: %s", kafka_exc)
                try:
                    producer.abort_transaction()
                except Exception as abort_exc:
                    log.error("[TXN]  abort failed: %s", abort_exc)
                total_txn_abort += 1
                log.warning("[TXN]  --- ABORTED %s ---", txn_label)
                time.sleep(1)

            except Exception as exc:
                log.exception("[TXN]  Unexpected error: %s", exc)
                try:
                    producer.abort_transaction()
                except Exception:
                    pass
                total_txn_abort += 1
                raise

    finally:
        log.info("[SHUTDOWN] Closing consumer ...")
        consumer.close()
        log.info("[SHUTDOWN] Consumer closed")

        if producer_state["producer"] is not None:
            log.info("[SHUTDOWN] Flushing producer txn.id='%s' ...",
                     producer_state["txn_id"])
            producer_state["producer"].flush(timeout=30)

        log.info("=" * 70)
        log.info("  FINAL SUMMARY  [instance-%s]", INSTANCE_ID)
        log.info("  Transactional ID : %s", producer_state.get("txn_id", "N/A"))
        log.info("  Consumed msgs    : %d", total_consumed)
        log.info("  Produced msgs    : %d", total_produced)
        log.info("  Txn Committed    : %d", total_txn_ok)
        log.info("  Txn Aborted      : %d", total_txn_abort)
        log.info("=" * 70)


if __name__ == "__main__":
    main()
