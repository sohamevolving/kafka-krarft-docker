#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
#  cassandra-init/init.sh
#
#  Robustly creates the kolkata keyspace + locations table.
#
#  Key design decisions vs the previous version:
#   1. Waits for CQL readiness on cassandra-1 (same as before)
#   2. Applies schema with CREATE IF NOT EXISTS (idempotent)
#   3. VERIFIES the keyspace is visible on BOTH nodes before exit
#      — a node can be UN in gossip but not yet ready for DDL
#   4. Exits non-zero on any failure so docker compose marks
#      cassandra-init as failed, blocking connector-init
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

PRIMARY_HOST="${PRIMARY_HOST:-cassandra-1}"
SECONDARY_HOST="${SECONDARY_HOST:-cassandra-2}"
CQL_PORT="${CQL_PORT:-9042}"
MAX_CQL_ATTEMPTS=40
MAX_VERIFY_ATTEMPTS=20
SLEEP_S=5

log()  { echo "[CASS-INIT] $(date '+%Y-%m-%d %H:%M:%S')  INFO   $*"; }
warn() { echo "[CASS-INIT] $(date '+%Y-%m-%d %H:%M:%S')  WARN   $*"; }
die()  { echo "[CASS-INIT] $(date '+%Y-%m-%d %H:%M:%S')  ERROR  $*" >&2; exit 1; }

# ── Helper: wait until a given host accepts a CQL statement ──────
wait_for_cql() {
    local host="$1"
    local label="$2"
    log "Waiting for CQL on ${label} (${host}:${CQL_PORT}) ..."
    for attempt in $(seq 1 "${MAX_CQL_ATTEMPTS}"); do
        if cqlsh "${host}" "${CQL_PORT}" \
                 -e "SELECT now() FROM system.local;" \
                 > /dev/null 2>&1; then
            log "  ${label} is accepting CQL  (attempt ${attempt}/${MAX_CQL_ATTEMPTS})"
            return 0
        fi
        if [ "${attempt}" -eq "${MAX_CQL_ATTEMPTS}" ]; then
            die "${label} not reachable after ${MAX_CQL_ATTEMPTS} attempts."
        fi
        warn "  ${label} not ready  (attempt ${attempt}/${MAX_CQL_ATTEMPTS}) – sleeping ${SLEEP_S}s ..."
        sleep "${SLEEP_S}"
    done
}

# ── Helper: verify a keyspace is visible on a given host ─────────
verify_keyspace_on_host() {
    local host="$1"
    local label="$2"
    log "Verifying keyspace 'kolkata' is visible on ${label} ..."
    for attempt in $(seq 1 "${MAX_VERIFY_ATTEMPTS}"); do
        COUNT=$(cqlsh "${host}" "${CQL_PORT}" \
            -e "SELECT keyspace_name FROM system_schema.keyspaces \
                WHERE keyspace_name='kolkata';" 2>/dev/null \
            | grep -c "kolkata" || true)
        if [ "${COUNT}" -ge 1 ]; then
            log "  Keyspace confirmed on ${label}  (attempt ${attempt})"
            return 0
        fi
        warn "  Keyspace not yet visible on ${label}  (attempt ${attempt}/${MAX_VERIFY_ATTEMPTS}) – sleeping ${SLEEP_S}s ..."
        sleep "${SLEEP_S}"
    done
    die "Keyspace 'kolkata' never became visible on ${label} after ${MAX_VERIFY_ATTEMPTS} attempts."
}

# ── Helper: verify a table is visible on a given host ────────────
verify_table_on_host() {
    local host="$1"
    local label="$2"
    log "Verifying table 'kolkata.locations' is visible on ${label} ..."
    for attempt in $(seq 1 "${MAX_VERIFY_ATTEMPTS}"); do
        COUNT=$(cqlsh "${host}" "${CQL_PORT}" \
            -e "SELECT table_name FROM system_schema.tables \
                WHERE keyspace_name='kolkata' AND table_name='locations';" \
            2>/dev/null | grep -c "locations" || true)
        if [ "${COUNT}" -ge 1 ]; then
            log "  Table confirmed on ${label}  (attempt ${attempt})"
            return 0
        fi
        warn "  Table not yet visible on ${label}  (attempt ${attempt}/${MAX_VERIFY_ATTEMPTS}) – sleeping ${SLEEP_S}s ..."
        sleep "${SLEEP_S}"
    done
    die "Table 'kolkata.locations' never became visible on ${label} after ${MAX_VERIFY_ATTEMPTS} attempts."
}

# ─────────────────────────────────────────────────────────────────
log "═══════════════════════════════════════════════════════"
log "  Cassandra Schema Init"
log "  Primary   : ${PRIMARY_HOST}:${CQL_PORT}"
log "  Secondary : ${SECONDARY_HOST}:${CQL_PORT}"
log "═══════════════════════════════════════════════════════"

# ── Phase 1: wait for both nodes to accept CQL ───────────────────
wait_for_cql "${PRIMARY_HOST}"   "cassandra-1 (primary)"
wait_for_cql "${SECONDARY_HOST}" "cassandra-2 (secondary)"

# ── Phase 2: apply schema via primary ────────────────────────────
log "Applying schema on ${PRIMARY_HOST} ..."

cqlsh "${PRIMARY_HOST}" "${CQL_PORT}" << 'CQLEOF'

-- ── Keyspace ────────────────────────────────────────────────────
-- RF=2 with NetworkTopologyStrategy on a 2-node datacenter1 ring.
-- LOCAL_QUORUM writes need ceil(RF/2)=1 ack → survives 1 node loss.
CREATE KEYSPACE IF NOT EXISTS kolkata
  WITH replication = {
    'class'       : 'NetworkTopologyStrategy',
    'datacenter1' : '2'
  }
  AND durable_writes = true;

-- ── Table ────────────────────────────────────────────────────────
-- Mirrors KolkataLocation Avro schema exactly.
-- 'landmark' is text (nullable) matching Avro union ["null","string"].
-- 'ts_epoch' is bigint (Unix ms).
CREATE TABLE IF NOT EXISTS kolkata.locations (
    id        int     PRIMARY KEY,
    name      text,
    zone      text,
    district  text,
    pincode   text,
    latitude  double,
    longitude double,
    landmark  text,
    ts_epoch  bigint
);

-- ── Secondary indexes ─────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_locations_zone
    ON kolkata.locations (zone);

CREATE INDEX IF NOT EXISTS idx_locations_district
    ON kolkata.locations (district);

CQLEOF

log "Schema DDL applied."

# ── Phase 3: verify keyspace + table on BOTH nodes ───────────────
# This is the critical step that was missing before.
# A node can be UN in gossip but not yet have propagated schema changes.
verify_keyspace_on_host "${PRIMARY_HOST}"   "cassandra-1 (primary)"
verify_keyspace_on_host "${SECONDARY_HOST}" "cassandra-2 (secondary)"

verify_table_on_host "${PRIMARY_HOST}"   "cassandra-1 (primary)"
verify_table_on_host "${SECONDARY_HOST}" "cassandra-2 (secondary)"

# ── Phase 4: final sanity – do a write + read round-trip ─────────
log "Running write/read round-trip sanity check ..."
cqlsh "${PRIMARY_HOST}" "${CQL_PORT}" -e \
  "INSERT INTO kolkata.locations (id, name, zone, district, pincode, latitude, longitude, ts_epoch)
   VALUES (-1, '__init_check__', 'INIT', 'INIT', '000000', 0.0, 0.0, 0)
   USING TTL 60;"

ROW=$(cqlsh "${PRIMARY_HOST}" "${CQL_PORT}" -e \
  "SELECT id FROM kolkata.locations WHERE id=-1;" 2>/dev/null | grep -c "\-1" || true)

if [ "${ROW}" -ge 1 ]; then
    log "Round-trip OK – write visible immediately on primary."
else
    die "Round-trip FAILED – inserted row not readable on primary."
fi

# ── Done ─────────────────────────────────────────────────────────
log "═══════════════════════════════════════════════════════"
log "  Schema init COMPLETE"
log "  Keyspace : kolkata  (RF=2, NetworkTopologyStrategy)"
log "  Table    : kolkata.locations"
log "  Indexes  : zone, district"
log "  Both cassandra-1 and cassandra-2 confirmed schema."
log "═══════════════════════════════════════════════════════"