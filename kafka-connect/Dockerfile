# ═══════════════════════════════════════════════════════════════════
#  Kafka Connect  +  DataStax Cassandra Sink Connector
#
#  Base  : confluentinc/cp-kafka-connect:7.6.0
#  Added : datastax/kafka-connect-cassandra-sink:1.4.0
#           (installed via confluent-hub at build time)
#
#  The DataStax connector ships with its own cassandra-driver,
#  so no extra JARs are needed.
# ═══════════════════════════════════════════════════════════════════
FROM confluentinc/cp-kafka-connect:7.6.0

# Install DataStax Kafka Connector from Confluent Hub.
# --no-prompt            → non-interactive
# --component-dir        → puts plugin under /usr/share/confluent-hub-components
#                          which is already in CONNECT_PLUGIN_PATH
RUN confluent-hub install --no-prompt \
      datastax/kafka-connect-cassandra-sink:1.4.0

# Optional: install Debezium Avro SMT support (useful for transforms)
# RUN confluent-hub install --no-prompt \
#       confluentinc/connect-transforms:latest

# ── Healthcheck so docker compose can wait on this service ───────
HEALTHCHECK --interval=15s --timeout=10s --retries=15 --start-period=90s \
  CMD curl -sf http://localhost:8083/ | grep -q version || exit 1