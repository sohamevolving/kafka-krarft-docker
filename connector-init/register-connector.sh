#!/bin/sh
# ═══════════════════════════════════════════════════════════════════
#  connector-init/register-connector.sh
#
#  cassandra-sink-connector.json stores the FLAT config map —
#  the inner object only, with no "name"/"config" wrapper.
#
#  REST API shape rules:
#    POST /connectors          → { "name": "...", "config": { ...flat... } }
#    PUT  /connectors/.../config  → { ...flat... }   ← no wrapper at all
#
#  This script reads the flat file and constructs the correct body
#  for whichever HTTP verb is needed, using only POSIX sh + printf.
# ═══════════════════════════════════════════════════════════════════
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_NAME="cassandra-sink-kolkata-locations"

# Flat config file — used as-is for PUT, wrapped for POST
FLAT_CONFIG="/connector-init/cassandra-sink-connector.json"

MAX_ATTEMPTS=40
SLEEP_S=5

log() { echo "[CONN-INIT] $(date '+%Y-%m-%dT%H:%M:%S')  $*"; }

# ── 1. Wait for Kafka Connect REST to respond ─────────────────────
log "Waiting for Kafka Connect at ${CONNECT_URL} ..."
for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
                     --connect-timeout 5 "${CONNECT_URL}/") || HTTP_CODE=0
    if [ "${HTTP_CODE}" = "200" ]; then
        log "Kafka Connect is UP (attempt ${attempt}/${MAX_ATTEMPTS})"
        break
    fi
    if [ "${attempt}" -eq "${MAX_ATTEMPTS}" ]; then
        log "ERROR: Kafka Connect did not become ready after ${MAX_ATTEMPTS} attempts."
        exit 1
    fi
    log "HTTP ${HTTP_CODE} – not ready (attempt ${attempt}/${MAX_ATTEMPTS}), sleeping ${SLEEP_S}s ..."
    sleep "${SLEEP_S}"
done

# ── 2. Register or update the connector ──────────────────────────
log "Checking whether connector '${CONNECTOR_NAME}' already exists ..."
EXISTS_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
                   "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")

FLAT_BODY=$(cat "${FLAT_CONFIG}")

if [ "${EXISTS_CODE}" = "200" ]; then
    # ── UPDATE via PUT /connectors/<name>/config ──────────────────
    # Body must be the FLAT config map — no "name" key, no "config" wrapper.
    log "Connector exists – updating via PUT /connectors/${CONNECTOR_NAME}/config ..."

    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X PUT \
        -H "Content-Type: application/json" \
        --data-binary "${FLAT_BODY}" \
        "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config")

    HTTP_CODE=$(echo "${RESPONSE}" | tail -n1)
    log "PUT /config returned HTTP ${HTTP_CODE}"

else
    # ── CREATE via POST /connectors ───────────────────────────────
    # Body must be { "name": "<name>", "config": { ...flat map... } }.
    # We wrap the flat file content at runtime using printf — no jq needed.
    log "Connector not found – creating via POST /connectors ..."

    POST_BODY=$(printf '{"name":"%s","config":%s}' "${CONNECTOR_NAME}" "${FLAT_BODY}")

    RESPONSE=$(curl -s -w "\n%{http_code}" \
        -X POST \
        -H "Content-Type: application/json" \
        --data-binary "${POST_BODY}" \
        "${CONNECT_URL}/connectors")

    HTTP_CODE=$(echo "${RESPONSE}" | tail -n1)
    log "POST /connectors returned HTTP ${HTTP_CODE}"
fi

# 200 = updated, 201 = created
if [ "${HTTP_CODE}" != "200" ] && [ "${HTTP_CODE}" != "201" ]; then
    log "ERROR: unexpected HTTP ${HTTP_CODE}"
    log "Response body:"
    echo "${RESPONSE}" | head -n -1   # print body (all lines except last status line)
    exit 1
fi
log "Connector registered/updated successfully."

# ── 3. Poll until all 6 tasks are RUNNING ────────────────────────
log "Waiting for all 6 connector tasks to reach RUNNING state ..."
TASK_ATTEMPTS=30
for attempt in $(seq 1 "${TASK_ATTEMPTS}"); do
    sleep 5

    STATUS_BODY=$(curl -s \
        "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || echo "")

    CONNECTOR_STATE=$(echo "${STATUS_BODY}" | \
        grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4 || echo "UNKNOWN")

    RUNNING_TASKS=$(echo "${STATUS_BODY}" | \
        grep -o '"state":"RUNNING"' | wc -l | tr -d ' ' || echo "0")

    log "Attempt ${attempt}/${TASK_ATTEMPTS}  connector=${CONNECTOR_STATE}  tasks_RUNNING=${RUNNING_TASKS}/6"

    if [ "${CONNECTOR_STATE}" = "RUNNING" ] && [ "${RUNNING_TASKS}" -ge "6" ]; then
        log "All 6 tasks are RUNNING. Sink is active."
        break
    fi

    if [ "${CONNECTOR_STATE}" = "FAILED" ]; then
        log "ERROR: Connector entered FAILED state. Full status:"
        echo "${STATUS_BODY}"
        exit 1
    fi

    if [ "${attempt}" -eq "${TASK_ATTEMPTS}" ]; then
        log "WARNING: not all tasks reached RUNNING within the wait window."
        log "Final status: ${STATUS_BODY}"
    fi
done

# ── 4. Summary ────────────────────────────────────────────────────
log "═══════════════════════════════════════════════════════"
log "  Connector : ${CONNECTOR_NAME}"
log "  Source    : kolkata-locations-mirror (6 partitions)"
log "  Sink      : cassandra-1:9042 / cassandra-2:9042"
log "  Keyspace  : kolkata   Table: locations"
log "  Tasks     : 6  (1 per partition, LOCAL_QUORUM)"
log "  DLQ topic : kolkata-locations-mirror-dlq"
log "═══════════════════════════════════════════════════════"
log "Useful commands:"
log "  # Live connector status"
log "  curl -s localhost:8083/connectors/${CONNECTOR_NAME}/status | python3 -m json.tool"
log "  # Query sinked rows"
log "  docker exec -it cassandra-1 cqlsh -e \\"
log "    \"SELECT id, name, zone FROM kolkata.locations LIMIT 20;\""
log "Done."