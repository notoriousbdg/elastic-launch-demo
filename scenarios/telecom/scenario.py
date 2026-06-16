"""Meridian Telecom 5G Platform scenario — European telco with 5G subscriber
activation, multi-cloud deployment, and MVNO partner integration."""

from __future__ import annotations

import random
import time
from typing import Any

from scenarios.base import BaseScenario, UITheme


class TelecomScenario(BaseScenario):
    """Meridian Telecom 5G activation platform with 9 services and 20 fault channels."""

    # ── Identity ──────────────────────────────────────────────────────

    @property
    def scenario_id(self) -> str:
        return "telecom"

    @property
    def scenario_name(self) -> str:
        return "Meridian Telecom 5G Platform"

    @property
    def scenario_description(self) -> str:
        return (
            "5G subscriber activation platform serving 45M subscribers across "
            "Consumer, Enterprise, and MVNO segments. European multi-cloud "
            "deployment spanning AWS Frankfurt, GCP Frankfurt, and Azure Netherlands."
        )

    @property
    def namespace(self) -> str:
        return "meridian"

    # ── Services ──────────────────────────────────────────────────────

    @property
    def services(self) -> dict[str, dict[str, Any]]:
        return {
            # AWS — Gateway & Core Activation
            "api-gateway": {
                "cloud_provider": "aws",
                "cloud_region": "eu-central-1",
                "cloud_platform": "aws_ec2",
                "cloud_availability_zone": "eu-central-1a",
                "subsystem": "gateway",
                "language": "go",
            },
            "activation-service": {
                "cloud_provider": "aws",
                "cloud_region": "eu-central-1",
                "cloud_platform": "aws_ec2",
                "cloud_availability_zone": "eu-central-1b",
                "subsystem": "activation",
                "language": "java",
            },
            "subscriber-db": {
                "cloud_provider": "aws",
                "cloud_region": "eu-central-1",
                "cloud_platform": "aws_ec2",
                "cloud_availability_zone": "eu-central-1a",
                "subsystem": "database",
                "language": "python",
            },
            # GCP — Provisioning & Network
            "provisioning-service": {
                "cloud_provider": "gcp",
                "cloud_region": "europe-west3",
                "cloud_platform": "gcp_compute_engine",
                "cloud_availability_zone": "europe-west3-a",
                "subsystem": "provisioning",
                "language": "java",
            },
            "network-core": {
                "cloud_provider": "gcp",
                "cloud_region": "europe-west3",
                "cloud_platform": "gcp_compute_engine",
                "cloud_availability_zone": "europe-west3-b",
                "subsystem": "network",
                "language": "cpp",
            },
            "notification-service": {
                "cloud_provider": "gcp",
                "cloud_region": "europe-west3",
                "cloud_platform": "gcp_compute_engine",
                "cloud_availability_zone": "europe-west3-a",
                "subsystem": "notifications",
                "language": "python",
            },
            # Azure — Auth & Billing
            "auth-service": {
                "cloud_provider": "azure",
                "cloud_region": "westeurope",
                "cloud_platform": "azure_vm",
                "cloud_availability_zone": "westeurope-1",
                "subsystem": "authentication",
                "language": "python",
            },
            "bss-billing": {
                "cloud_provider": "azure",
                "cloud_region": "westeurope",
                "cloud_platform": "azure_vm",
                "cloud_availability_zone": "westeurope-2",
                "subsystem": "billing",
                "language": "java",
            },
            "identity-db": {
                "cloud_provider": "azure",
                "cloud_region": "westeurope",
                "cloud_platform": "azure_vm",
                "cloud_availability_zone": "westeurope-1",
                "subsystem": "database",
                "language": "python",
            },
        }

    # ── Channel Registry ──────────────────────────────────────────────

    @property
    def channel_registry(self) -> dict[int, dict[str, Any]]:
        return {
            1: {
                "name": "DB Connection Pool Exhaustion",
                "subsystem": "database",
                "vehicle_section": "subscriber_data",
                "error_type": "DB-CONNPOOL-EXHAUST",
                "sensor_type": "connection_pool",
                "affected_services": ["subscriber-db", "provisioning-service"],
                "cascade_services": ["activation-service", "api-gateway"],
                "description": "PostgreSQL connection pool on subscriber-db saturated causing query timeouts across provisioning pipeline",
                "investigation_notes": (
                    "1. Check HikariCP pool metrics on provisioning-service — pool_active near pool_max (100) indicates "
                    "exhaustion. Run: kubectl exec provisioning-service -- curl localhost:8080/actuator/metrics/hikaricp.connections.active\n"
                    "2. Review pg_stat_activity on subscriber-db for long-running queries: SELECT pid, now()-query_start AS duration, "
                    "query FROM pg_stat_activity WHERE state='active' ORDER BY duration DESC LIMIT 20.\n"
                    "3. Check for vacuum or reindex operations that may be holding locks on the subscribers table (45M rows). "
                    "Long-running maintenance on large tables is the most common root cause of pool exhaustion.\n"
                    "4. Review CloudTrail for recent ModifyDBInstance events on subscriber-db RDS instance — automated "
                    "maintenance windows can trigger vacuum operations without operator awareness.\n"
                    "5. Monitor connection wait time — if avg_wait_ms exceeds 3000ms, provisioning-service will begin "
                    "returning 503s to activation-service. Check: SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Client'."
                ),
                "remediation_action": "restart_subscriber_db_pool",
                "error_message": "[DB] DB-CONNPOOL-EXHAUST: pool_active={pool_active} pool_max={pool_max} wait_queue={wait_queue} avg_wait_ms={avg_wait_ms} db_host={db_host}",
                "stack_trace": (
                    "=== SUBSCRIBER DB CONNECTION POOL REPORT ===\n"
                    "db_host={db_host}  pool=HikariCP  table=subscribers (45M rows)\n"
                    "--- POOL STATUS ---\n"
                    "  METRIC                VALUE       THRESHOLD    STATUS\n"
                    "  active_connections     {pool_active}        100          EXHAUSTED\n"
                    "  idle_connections       0           10           DEPLETED\n"
                    "  wait_queue             {wait_queue}        50           OVERFLOW\n"
                    "  avg_wait_ms            {avg_wait_ms}      3000         CRITICAL\n"
                    "--- BLOCKING QUERIES ---\n"
                    "  pid=14829  duration=847s  query='VACUUM ANALYZE subscribers'\n"
                    "  pid=14830  duration=612s  query='REINDEX INDEX idx_subscribers_msisdn'\n"
                    "--- CASCADE IMPACT ---\n"
                    "  provisioning-service: 503 errors, timeout after 3000ms\n"
                    "  activation-service: upstream timeout, retry storm (3x multiplier)\n"
                    "  api-gateway: 502/503 to customers, {error_rate_pct}% error rate\n"
                    "ACTION: cancel_maintenance=true  kill_long_queries=true  alert=DB-CONNPOOL-EXHAUST"
                ),
                "infra_impact": {"cpu_pct": 95, "memory_pct": 80, "latency_multiplier": 3.0},
                "infrastructure_events": [
                    {
                        "body": "[CloudWatch] RDS ConnectionPoolExhausted: instance=subscriber-db pool_active=100/100 wait_queue=350 avg_wait_ms=4200 alarm=ALARM region=eu-central-1",
                        "severity": "ERROR",
                        "event_name": "aws.cloudwatch.alarm.ConnectionPoolExhausted",
                        "service": "subscriber-db",
                        "attributes": {
                            "cloud.provider": "aws",
                            "cloud.service": "cloudwatch",
                            "aws.cloudwatch.alarm_name": "subscriber-db-pool-exhaustion",
                            "aws.cloudwatch.state": "ALARM",
                            "aws.rds.instance": "subscriber-db",
                            "infrastructure.source": "CloudWatch",
                        },
                    },
                ],
            },
            2: {
                "name": "RDS Automated Maintenance Spike",
                "subsystem": "database",
                "vehicle_section": "subscriber_data",
                "error_type": "DB-RDS-MAINTENANCE",
                "sensor_type": "db_latency",
                "affected_services": ["subscriber-db", "provisioning-service"],
                "cascade_services": ["activation-service", "bss-billing"],
                "description": "AWS RDS automated maintenance on subscriber-db causing 200x latency increase on query operations",
                "investigation_notes": (
                    "1. Check CloudTrail for ModifyDBInstance events — RDS maintenance windows trigger VACUUM ANALYZE on "
                    "the subscribers table (45M rows), causing 15-25 minute lock periods with 100-200x latency increase.\n"
                    "2. Monitor subscriber-db query latency: baseline 15-25ms should not exceed 500ms. Current readings "
                    "above 4000ms indicate active maintenance lock on the subscribers table.\n"
                    "3. Historical pattern: 2025-08-14 (12K failed activations), 2025-11-02 (28min outage), 2026-01-18 "
                    "(8min degradation). All correlated with RDS maintenance window events.\n"
                    "4. Provisioning-service HikariCP connection timeout is 3000ms — once DB latency exceeds this, all "
                    "provisioning requests fail, creating a cascade to activation-service and api-gateway.\n"
                    "5. Immediate action: Cancel active maintenance via AWS CLI: aws rds modify-db-instance "
                    "--db-instance-identifier subscriber-db --no-apply-immediately. Monitor latency recovery (typically 2-5min)."
                ),
                "remediation_action": "cancel_rds_maintenance",
                "error_message": "[DB] DB-RDS-MAINTENANCE: latency_ms={db_latency_ms} baseline_ms={db_baseline_ms} event={rds_event} duration_min={maintenance_duration_min} table=subscribers",
                "stack_trace": (
                    "=== RDS MAINTENANCE EVENT REPORT ===\n"
                    "instance=subscriber-db  engine=PostgreSQL 15.4  table=subscribers (45M rows)\n"
                    "--- CLOUDTRAIL EVENT ---\n"
                    "  event_name=ModifyDBInstance  source=rds.amazonaws.com\n"
                    "  event_time={event_time}  region=eu-central-1\n"
                    "  operation=VACUUM ANALYZE  target_table=subscribers\n"
                    "--- LATENCY IMPACT ---\n"
                    "  METRIC              BASELINE     CURRENT      MULTIPLIER\n"
                    "  query_latency_ms    {db_baseline_ms}          {db_latency_ms}         {latency_multiplier}x\n"
                    "  connections_active  45           98           2.2x\n"
                    "  lock_wait_ms        0            {db_latency_ms}         ---\n"
                    "--- SUBSCRIBER IMPACT ---\n"
                    "  failed_activations={failed_activations}  affected_subscribers=~{affected_subscribers}\n"
                    "  duration={maintenance_duration_min}min  estimated_remaining=5-10min\n"
                    "ACTION: cancel_maintenance=true  monitor_latency_recovery=true  alert=DB-RDS-MAINTENANCE"
                ),
                "infra_impact": {"cpu_pct": 90, "memory_pct": 75, "latency_multiplier": 5.0},
                "infrastructure_events": [
                    {
                        "body": "[CloudTrail] ModifyDBInstance: instance=subscriber-db engine=PostgreSQL operation=VACUUM_ANALYZE initiated_by=RDSAutoMaintenance region=eu-central-1 event_source=rds.amazonaws.com",
                        "severity": "WARN",
                        "event_name": "aws.cloudtrail.ModifyDBInstance",
                        "service": "subscriber-db",
                        "attributes": {
                            "cloud.provider": "aws",
                            "cloud.service": "rds",
                            "aws.cloudtrail.event_name": "ModifyDBInstance",
                            "aws.rds.instance": "subscriber-db",
                            "aws.rds.operation": "VACUUM_ANALYZE",
                            "infrastructure.source": "CloudTrail",
                        },
                    },
                ],
            },
            3: {
                "name": "Identity DB Replication Lag",
                "subsystem": "database",
                "vehicle_section": "identity_data",
                "error_type": "DB-REPLICATION-LAG",
                "sensor_type": "replication",
                "affected_services": ["identity-db", "auth-service"],
                "cascade_services": ["api-gateway"],
                "description": "Azure PostgreSQL cross-region replication lag causing stale authentication reads and session failures",
                "investigation_notes": (
                    "1. Check Azure PostgreSQL replication metrics — replica lag exceeding 5000ms causes stale session reads "
                    "where auth-service validates expired tokens as active.\n"
                    "2. WAL replay rate deficit indicates the replica cannot keep up with write throughput. Check disk IOPS "
                    "saturation on the replica: az postgres flexible-server show --name identity-db-replica.\n"
                    "3. Auth-service reads from replica for session validation — stale reads cause 30% of sessions to appear "
                    "invalid, triggering unnecessary re-authentication storms on the primary.\n"
                    "4. Route all reads to primary immediately: UPDATE pg_settings SET setting='primary' WHERE name='read_routing'. "
                    "This increases primary load but prevents stale authentication data.\n"
                    "5. Scale replica compute tier: az postgres flexible-server update --sku-name Standard_E4s_v3 to increase "
                    "IOPS capacity and reduce WAL replay deficit."
                ),
                "remediation_action": "failover_identity_db",
                "error_message": "[DB] DB-REPLICATION-LAG: lag_ms={repl_lag_ms} max_ms={repl_max_ms} replica={repl_node} pending_txns={repl_pending_txns} stale_reads_pct={stale_reads_pct}%",
                "stack_trace": (
                    "=== IDENTITY DB REPLICATION STATUS ===\n"
                    "cluster=identity-db  engine=Azure_PostgreSQL_15\n"
                    "--- REPLICATION TOPOLOGY ---\n"
                    "  NODE                ROLE        LAG_MS    STATUS\n"
                    "  identity-db-primary PRIMARY     0         OK\n"
                    "  identity-db-replica REPLICA     {repl_lag_ms}     BEHIND  <<< ALERT\n"
                    "--- LAG ANALYSIS ---\n"
                    "  current_lag={repl_lag_ms}ms  max_allowed={repl_max_ms}ms  breach=true\n"
                    "  pending_txns={repl_pending_txns}  wal_send_rate=8MB/s  wal_replay_rate=5MB/s\n"
                    "  disk_iops=SATURATED  cpu_replica=91%\n"
                    "--- AUTH IMPACT ---\n"
                    "  stale_session_reads={stale_reads_pct}%  invalid_token_rate=elevated\n"
                    "  reauth_storm_risk=HIGH  read_routing=ROUND_ROBIN\n"
                    "ACTION: route_reads_primary=true  scale_replica=true  alert=DB-REPLICATION-LAG"
                ),
                "infrastructure_events": [
                    {
                        "body": "[AzureHealth] ReplicationLagExceeded: resource=identity-db-replica lag_ms=15000 threshold_ms=5000 region=westeurope cause=IOPS_SATURATION",
                        "severity": "WARN",
                        "event_name": "azure.health.ReplicationLagExceeded",
                        "service": "identity-db",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "postgresql",
                            "azure.health.status": "Degraded",
                            "azure.health.cause": "IOPS_SATURATION",
                            "infrastructure.source": "AzureHealth",
                        },
                    },
                ],
            },
            4: {
                "name": "5G Bearer Allocation Timeout",
                "subsystem": "network",
                "vehicle_section": "radio_access",
                "error_type": "NET-BEARER-TIMEOUT",
                "sensor_type": "bearer_allocation",
                "affected_services": ["network-core", "provisioning-service"],
                "cascade_services": ["activation-service", "api-gateway"],
                "description": "5G NR bearer setup requests timing out due to gNodeB resource exhaustion in target cell",
                "investigation_notes": (
                    "1. Check gNodeB resource utilization for the target cell — PRB (Physical Resource Block) usage above "
                    "95% prevents new bearer allocation. Query: network-core metrics for gnb.prb_utilization_pct.\n"
                    "2. Bearer setup involves S1-AP InitialContextSetupRequest → gNodeB resource allocation → E-RAB setup. "
                    "Timeout at the gNodeB allocation step indicates radio resource congestion, not core network issues.\n"
                    "3. Review concurrent bearer count per cell — max 256 active bearers per cell. If at capacity, new "
                    "activations queue until existing bearers are released (average bearer lifetime 45min).\n"
                    "4. Check for mass event scenarios (stadium, convention) that concentrate subscribers in a single cell. "
                    "Enable carrier aggregation or activate temporary small cells if available.\n"
                    "5. Provisioning-service retries bearer allocation 3x with exponential backoff (1s, 2s, 4s) before "
                    "returning failure to activation-service. Monitor retry success rate to gauge recovery trajectory."
                ),
                "remediation_action": "reset_bearer_allocation",
                "error_message": "[NET] NET-BEARER-TIMEOUT: cell_id={cell_id} bearer_type={bearer_type} timeout_ms={bearer_timeout_ms} prb_util={prb_util_pct}% active_bearers={active_bearers}",
                "stack_trace": (
                    "=== 5G BEARER ALLOCATION REPORT ===\n"
                    "cell_id={cell_id}  gnodeb={gnodeb_id}  sector=3\n"
                    "--- ALLOCATION PIPELINE ---\n"
                    "  STEP                    STATUS      ELAPSED_MS\n"
                    "  s1ap_initial_context    COMPLETE    42\n"
                    "  gnb_resource_check      COMPLETE    18\n"
                    "  prb_allocation          TIMEOUT     {bearer_timeout_ms}  <<< STUCK\n"
                    "  erab_setup              SKIPPED     ---\n"
                    "  bearer_activation       SKIPPED     ---\n"
                    "--- CELL RESOURCES ---\n"
                    "  prb_utilization={prb_util_pct}%  active_bearers={active_bearers}/256\n"
                    "  carrier_aggregation=DISABLED  small_cell_available=false\n"
                    "  bearer_type={bearer_type}  qos_class=QCI-9\n"
                    "--- SUBSCRIBER IMPACT ---\n"
                    "  queued_activations={queued_activations}  avg_wait_s=45\n"
                    "ACTION: enable_ca=true  redistribute_load=true  alert=NET-BEARER-TIMEOUT"
                ),
                "infra_impact": {"cpu_pct": 85, "latency_multiplier": 2.0},
                "infrastructure_events": [
                    {
                        "body": "[GCP-Monitoring] BearerAllocationTimeout: cluster=network-core-gke pod=bearer-allocator-7f9a latency_p99=12000ms threshold=5000ms region=europe-west3",
                        "severity": "WARN",
                        "event_name": "gcp.monitoring.BearerAllocationTimeout",
                        "service": "network-core",
                        "attributes": {
                            "cloud.provider": "gcp",
                            "cloud.service": "monitoring",
                            "gcp.resource_type": "gke_container",
                            "infrastructure.source": "GCP-Monitoring",
                        },
                    },
                ],
            },
            5: {
                "name": "S1-MME Interface Congestion",
                "subsystem": "network",
                "vehicle_section": "core_network",
                "error_type": "NET-S1MME-CONGEST",
                "sensor_type": "signaling",
                "affected_services": ["network-core", "provisioning-service"],
                "cascade_services": ["notification-service"],
                "description": "S1-MME signaling interface congested causing attach/detach procedure delays across MME pool",
                "investigation_notes": (
                    "1. Check MME signaling load — S1-MME SCTP association handling above 85% capacity causes queuing of "
                    "NAS messages (Attach, TAU, Service Request). Monitor: mme.sctp_association_count and mme.nas_msg_queue.\n"
                    "2. Identify the source of signaling surge — mass TAU (Tracking Area Update) storms from moving subscribers "
                    "(trains, highways) can spike signaling 10-20x above baseline.\n"
                    "3. Review MME pool load balancing — if one MME in the pool is receiving disproportionate signaling load, "
                    "DNS-based S1-MME distribution may need rebalancing.\n"
                    "4. Check for paging storms — excessive paging for idle-mode subscribers (e.g., push notification bursts) "
                    "consumes S1-MME capacity. Coordinate with notification-service to throttle.\n"
                    "5. Enable S1-MME overload control: send OVERLOAD START to eNodeBs to trigger reject-non-emergency "
                    "procedures. This preserves capacity for emergency calls and critical activations."
                ),
                "remediation_action": "rebalance_mme_pool",
                "error_message": "[NET] NET-S1MME-CONGEST: mme_id={mme_id} sctp_load={sctp_load_pct}% nas_queue={nas_queue_depth} attach_delay_ms={attach_delay_ms} region={mme_region}",
                "stack_trace": (
                    "=== S1-MME INTERFACE STATUS ===\n"
                    "mme_id={mme_id}  region={mme_region}  pool=MME-POOL-EU-CENTRAL\n"
                    "--- SIGNALING METRICS ---\n"
                    "  METRIC               VALUE       CAPACITY    STATUS\n"
                    "  sctp_associations     {sctp_load_pct}%       85%         CONGESTED\n"
                    "  nas_msg_queue         {nas_queue_depth}       1000        OVERFLOW\n"
                    "  attach_procedures     4,200/min   5,000/min   WARNING\n"
                    "  tau_procedures        8,400/min   6,000/min   EXCEEDED  <<< SURGE\n"
                    "--- PROCEDURE DELAYS ---\n"
                    "  attach_avg_ms={attach_delay_ms}  baseline=50ms  multiplier={attach_multiplier}x\n"
                    "  tau_avg_ms=1,200    baseline=30ms\n"
                    "  service_request_avg_ms=800  baseline=20ms\n"
                    "ACTION: overload_start=true  rebalance_pool=true  alert=NET-S1MME-CONGEST"
                ),
                "infrastructure_events": [
                    {
                        "body": "[GCP-Monitoring] SCTPAssociationDegraded: interface=S1-MME load_pct=95 nas_queue=500 region=europe-west3 cluster=network-core-gke",
                        "severity": "WARN",
                        "event_name": "gcp.monitoring.SCTPAssociationDegraded",
                        "service": "network-core",
                        "attributes": {
                            "cloud.provider": "gcp",
                            "cloud.service": "monitoring",
                            "gcp.resource_type": "gke_container",
                            "infrastructure.source": "GCP-Monitoring",
                        },
                    },
                ],
            },
            6: {
                "name": "GTP Tunnel Setup Failure",
                "subsystem": "network",
                "vehicle_section": "packet_core",
                "error_type": "NET-GTP-TUNNEL-FAIL",
                "sensor_type": "tunnel_setup",
                "affected_services": ["network-core"],
                "cascade_services": ["activation-service", "bss-billing"],
                "description": "GTP-C/GTP-U tunnel establishment failing between SGW and PGW causing data path setup failures",
                "investigation_notes": (
                    "1. Check PGW GTP-C response status — Create Session Response with cause NOT_ACCEPTED indicates PGW "
                    "resource exhaustion. Monitor: pgw.active_pdp_contexts and pgw.gtp_tunnel_count.\n"
                    "2. GTP tunnel setup requires TEID (Tunnel Endpoint Identifier) allocation on both SGW and PGW. TEID "
                    "pool exhaustion on either node prevents new tunnel creation.\n"
                    "3. Review GTP path failure detection — GTP echo request/response timeout (3x missed) triggers path "
                    "failure, tearing down all tunnels on the affected path. Check for intermittent network connectivity.\n"
                    "4. APN configuration mismatch can cause tunnel setup rejection — verify APN 'internet.meridian.eu' "
                    "is correctly configured on both SGW and PGW. Check: show apn configuration internet.meridian.eu.\n"
                    "5. Impact assessment: GTP tunnel failure prevents data connectivity for activated subscribers. Bearer "
                    "allocation succeeds (radio path OK) but user plane data cannot flow without the GTP tunnel."
                ),
                "remediation_action": "reset_gtp_tunnels",
                "error_message": "[NET] NET-GTP-TUNNEL-FAIL: tunnel_id={tunnel_id} sgw={sgw_id} pgw={pgw_id} cause={gtp_cause} teid_pool_util={teid_util_pct}%",
                "stack_trace": (
                    "=== GTP TUNNEL SETUP FAILURE ===\n"
                    "tunnel_id={tunnel_id}  apn=internet.meridian.eu\n"
                    "--- TUNNEL SETUP SEQUENCE ---\n"
                    "  STEP                        STATUS      DETAIL\n"
                    "  create_session_request       SENT        sgw={sgw_id} → pgw={pgw_id}\n"
                    "  teid_allocation_sgw          COMPLETE    teid_sgw=0x{teid_sgw}\n"
                    "  teid_allocation_pgw          FAILED      {gtp_cause}  <<< REJECTED\n"
                    "  create_session_response      ERROR       cause=RESOURCE_UNAVAILABLE\n"
                    "  gtp_u_path_setup             SKIPPED     ---\n"
                    "--- NODE STATUS ---\n"
                    "  sgw_id={sgw_id}  active_tunnels=48,200  capacity=50,000\n"
                    "  pgw_id={pgw_id}  teid_pool_util={teid_util_pct}%  pdp_contexts=SATURATED\n"
                    "  gtp_echo_status=OK  path_failure=false\n"
                    "ACTION: clear_stale_tunnels=true  scale_pgw=true  alert=NET-GTP-TUNNEL-FAIL"
                ),
                "infrastructure_events": [
                    {
                        "body": "[GCP-Monitoring] GTPTunnelSetupFailure: tunnel_type=GTPv2-C failure_rate=35% cause=RESOURCE_UNAVAILABLE pgw=network-core-pgw region=europe-west3",
                        "severity": "ERROR",
                        "event_name": "gcp.monitoring.GTPTunnelSetupFailure",
                        "service": "network-core",
                        "attributes": {
                            "cloud.provider": "gcp",
                            "cloud.service": "monitoring",
                            "gcp.resource_type": "gke_container",
                            "infrastructure.source": "GCP-Monitoring",
                        },
                    },
                ],
            },
            7: {
                "name": "OAuth Token Validation Failure",
                "subsystem": "authentication",
                "vehicle_section": "auth_pipeline",
                "error_type": "AUTH-TOKEN-INVALID",
                "sensor_type": "token_validation",
                "affected_services": ["auth-service", "identity-db"],
                "cascade_services": ["api-gateway", "activation-service"],
                "description": "OAuth2 token validation failing due to JWKS key rotation mismatch between auth-service and identity provider",
                "investigation_notes": (
                    "1. Check JWKS endpoint response — if the identity provider rotated signing keys but auth-service cached "
                    "the old JWKS, all token validations fail with INVALID_SIGNATURE. Verify: curl https://idp.meridian.eu/.well-known/jwks.json.\n"
                    "2. Auth-service JWKS cache TTL is 3600s — if keys rotated mid-TTL, stale keys are used for validation. "
                    "Force cache refresh: POST /api/v1/auth/jwks/refresh on auth-service.\n"
                    "3. Check clock skew between auth-service and identity-db — token expiry validation is sensitive to clock "
                    "drift. NTP sync status: timedatectl show | grep NTPSynchronized.\n"
                    "4. Review token issuer claim (iss) — multiple identity providers (internal, Azure AD, MVNO partner) may "
                    "have different JWKS endpoints. Ensure all issuers are registered in auth-service config.\n"
                    "5. Impact: all API requests through api-gateway require valid tokens. Token validation failures cause "
                    "401 responses, blocking all activation and provisioning operations."
                ),
                "remediation_action": "refresh_jwks_cache",
                "error_message": "[AUTH] AUTH-TOKEN-INVALID: issuer={token_issuer} error={token_error} cache_age_s={jwks_cache_age_s} kid={token_kid} affected_requests={auth_affected_requests}",
                "stack_trace": (
                    "=== OAUTH TOKEN VALIDATION FAILURE ===\n"
                    "issuer={token_issuer}  kid={token_kid}\n"
                    "--- VALIDATION PIPELINE ---\n"
                    "  STEP                    STATUS      DETAIL\n"
                    "  token_parse             COMPLETE    JWT format valid\n"
                    "  issuer_check            COMPLETE    issuer={token_issuer}\n"
                    "  jwks_lookup             COMPLETE    cache_age={jwks_cache_age_s}s\n"
                    "  signature_verify        FAILED      {token_error}  <<< MISMATCH\n"
                    "  claims_validate         SKIPPED     ---\n"
                    "  scope_check             SKIPPED     ---\n"
                    "--- JWKS STATUS ---\n"
                    "  cache_ttl=3600s  cache_age={jwks_cache_age_s}s  keys_cached=2\n"
                    "  idp_keys_current=3  mismatch=true  rotation_detected=true\n"
                    "  affected_requests={auth_affected_requests}  reject_rate=100%\n"
                    "ACTION: force_jwks_refresh=true  clear_token_cache=true  alert=AUTH-TOKEN-INVALID"
                ),
                "infrastructure_events": [
                    {
                        "body": "[AzureHealth] TokenValidationDegraded: resource=auth-service error_rate=45% jwks_cache=STALE provider=AzureAD region=westeurope",
                        "severity": "WARN",
                        "event_name": "azure.health.TokenValidationDegraded",
                        "service": "auth-service",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "active_directory",
                            "azure.health.status": "Degraded",
                            "infrastructure.source": "AzureHealth",
                        },
                    },
                ],
            },
            8: {
                "name": "AD Federation Timeout",
                "subsystem": "authentication",
                "vehicle_section": "identity_federation",
                "error_type": "AUTH-FEDERATION-TIMEOUT",
                "sensor_type": "federation",
                "affected_services": ["auth-service"],
                "cascade_services": ["api-gateway"],
                "description": "Azure AD federation SAML/OIDC response timeout preventing enterprise SSO authentication",
                "investigation_notes": (
                    "1. Check Azure AD health dashboard — federation timeouts typically correlate with Azure AD regional "
                    "outages or certificate rotation. Monitor: https://status.azure.com/en-us/status.\n"
                    "2. SAML assertion response timeout at 30s indicates the Azure AD token endpoint is not responding. "
                    "Verify network connectivity: curl -w '%%{time_total}' https://login.microsoftonline.com/{tenant_id}/saml2.\n"
                    "3. Enterprise SSO users (20% of traffic) are completely blocked when federation is down. Consumer and "
                    "MVNO users using local auth are unaffected — impact is limited to enterprise segment.\n"
                    "4. Enable fallback to cached SAML assertions for recently-authenticated enterprise users. Auth-service "
                    "maintains a 15-minute assertion cache that can extend active sessions during federation outages.\n"
                    "5. For new enterprise login attempts, display a maintenance message directing users to contact their "
                    "enterprise admin. Do not expose Azure AD error details to end users."
                ),
                "remediation_action": "enable_federation_fallback",
                "error_message": "[AUTH] AUTH-FEDERATION-TIMEOUT: provider={federation_provider} timeout_ms={federation_timeout_ms} tenant={federation_tenant} sso_users_blocked={sso_blocked_count}",
                "stack_trace": (
                    "=== AD FEDERATION STATUS ===\n"
                    "provider={federation_provider}  tenant={federation_tenant}\n"
                    "--- FEDERATION PIPELINE ---\n"
                    "  STEP                    STATUS      ELAPSED_MS\n"
                    "  saml_request_build      COMPLETE    12\n"
                    "  idp_redirect            COMPLETE    85\n"
                    "  saml_response_wait      TIMEOUT     {federation_timeout_ms}  <<< STUCK\n"
                    "  assertion_validate      SKIPPED     ---\n"
                    "  session_create          SKIPPED     ---\n"
                    "--- AZURE AD STATUS ---\n"
                    "  endpoint=login.microsoftonline.com  response=TIMEOUT\n"
                    "  region=westeurope  status_page=DEGRADED\n"
                    "  sso_users_blocked={sso_blocked_count}  enterprise_impact=100%\n"
                    "  consumer_impact=0%  mvno_impact=0%\n"
                    "ACTION: enable_assertion_cache=true  display_maintenance=true  alert=AUTH-FEDERATION-TIMEOUT"
                ),
                "infrastructure_events": [
                    {
                        "body": "[AzureHealth] ResourceHealthChanged: resource=auth-service status=Degraded cause=AzureAD_Federation_Timeout region=westeurope timestamp=2026-03-08T09:12:00Z",
                        "severity": "WARN",
                        "event_name": "azure.health.ResourceHealthChanged",
                        "service": "auth-service",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "active_directory",
                            "azure.health.status": "Degraded",
                            "azure.health.cause": "AzureAD_Federation_Timeout",
                            "infrastructure.source": "AzureHealth",
                        },
                    },
                ],
            },
            9: {
                "name": "Session Store Redis Saturation",
                "subsystem": "authentication",
                "vehicle_section": "session_store",
                "error_type": "AUTH-SESSION-SATURATE",
                "sensor_type": "session_mgmt",
                "affected_services": ["auth-service", "identity-db"],
                "cascade_services": ["api-gateway", "activation-service"],
                "description": "Redis session store memory saturation causing session eviction and mass re-authentication storms",
                "investigation_notes": (
                    "1. Check Redis memory usage: redis-cli INFO memory | grep used_memory_human. Memory at 100% triggers "
                    "LRU eviction of active sessions, causing mass re-authentication.\n"
                    "2. Session count growth rate — if new sessions exceed eviction rate, Redis enters thrashing mode. "
                    "Current session count: redis-cli DBSIZE. Expected baseline: ~500K sessions.\n"
                    "3. Thundering herd problem: evicted sessions trigger re-authentication on identity-db, which cannot "
                    "handle the spike. Auth-service circuit breaker opens at 1000 req/s to protect identity-db.\n"
                    "4. Increase Redis maxmemory immediately: redis-cli CONFIG SET maxmemory 8gb. Then investigate why "
                    "session count exceeded capacity (possible session leak, missing TTL on new session types).\n"
                    "5. Review session TTL distribution — sessions without explicit TTL accumulate indefinitely. "
                    "Ensure all session types have TTL <= 3600s: redis-cli --scan --pattern 'session:*' | head -20 | xargs -I{} redis-cli TTL {}."
                ),
                "remediation_action": "flush_expired_sessions",
                "error_message": "[AUTH] AUTH-SESSION-SATURATE: memory_used_pct={redis_memory_pct}% sessions={redis_session_count} evictions={redis_evictions}/min reauth_rate={reauth_rate}/min",
                "stack_trace": (
                    "=== REDIS SESSION STORE STATUS ===\n"
                    "cluster=auth-redis-cluster  mode=sentinel\n"
                    "--- MEMORY STATUS ---\n"
                    "  used_memory={redis_memory_pct}%  maxmemory=4GB\n"
                    "  eviction_policy=allkeys-lru  evictions={redis_evictions}/min\n"
                    "  sessions_active={redis_session_count}  sessions_baseline=500000\n"
                    "--- EVICTION CASCADE ---\n"
                    "  T+0s    memory_threshold_exceeded\n"
                    "  T+2s    lru_eviction_started  rate={redis_evictions}/min\n"
                    "  T+5s    reauth_storm  rate={reauth_rate}/min\n"
                    "  T+8s    identity-db connection spike\n"
                    "  T+12s   auth-service circuit_breaker=OPEN\n"
                    "--- IMPACT ---\n"
                    "  api-gateway: 401 responses, all segments affected\n"
                    "  activation-service: blocked on auth, queue building\n"
                    "ACTION: increase_maxmemory=true  flush_expired=true  alert=AUTH-SESSION-SATURATE"
                ),
                "infrastructure_events": [
                    {
                        "body": "[AzureHealth] RedisCacheMemorySaturation: resource=meridian-session-cache memory_used_pct=98 eviction_policy=volatile-lru evictions_per_sec=450 region=westeurope",
                        "severity": "ERROR",
                        "event_name": "azure.health.RedisCacheMemorySaturation",
                        "service": "auth-service",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "redis_cache",
                            "azure.health.status": "Critical",
                            "azure.health.cause": "MEMORY_SATURATION",
                            "infrastructure.source": "AzureHealth",
                        },
                    },
                ],
            },
            10: {
                "name": "SIM Profile Provisioning Stall",
                "subsystem": "provisioning",
                "vehicle_section": "sim_management",
                "error_type": "PROV-SIM-STALL",
                "sensor_type": "sim_provisioning",
                "affected_services": ["provisioning-service", "subscriber-db"],
                "cascade_services": ["activation-service", "notification-service"],
                "description": "OTA SIM profile provisioning pipeline stalled due to HSS/HLR registration queue overflow",
                "investigation_notes": (
                    "1. Check HSS registration queue depth — queue above 5000 indicates HLR is not processing registrations "
                    "fast enough. Monitor: provisioning-service metrics for hlr.registration_queue_depth.\n"
                    "2. SIM OTA provisioning requires: ICCID validation → subscriber-db profile lookup → HLR registration "
                    "→ SIM profile download. The stall is at HLR registration step.\n"
                    "3. Review HLR processing rate — baseline 200 registrations/min should handle normal activation volume. "
                    "If processing rate drops below 50/min, check HLR backend connectivity.\n"
                    "4. eSIM activations are also affected — remote SIM provisioning (RSP) uses the same HLR registration "
                    "path. eSIM QR code generation succeeds but profile activation fails.\n"
                    "5. Subscriber-db writes are queuing because provisioning-service cannot complete the activation cycle. "
                    "Monitor activation_log table for INSERT queue growth."
                ),
                "remediation_action": "restart_hlr_registration",
                "error_message": "[PROV] PROV-SIM-STALL: iccid={sim_iccid} msisdn={msisdn} hlr_queue={hlr_queue_depth} processing_rate={hlr_processing_rate}/min stage={sim_stage}",
                "stack_trace": (
                    "=== SIM PROVISIONING PIPELINE ===\n"
                    "msisdn={msisdn}  iccid={sim_iccid}  type={sim_type}\n"
                    "--- PROVISIONING STAGES ---\n"
                    "  STAGE                STATUS      ELAPSED_MS\n"
                    "  iccid_validation     COMPLETE    45\n"
                    "  subscriber_lookup    COMPLETE    120\n"
                    "  profile_generation   COMPLETE    350\n"
                    "  hlr_registration     STALLED     {hlr_stall_ms}  <<< QUEUE OVERFLOW\n"
                    "  sim_ota_download     PENDING     ---\n"
                    "  activation_confirm   PENDING     ---\n"
                    "--- HLR STATUS ---\n"
                    "  queue_depth={hlr_queue_depth}  processing_rate={hlr_processing_rate}/min\n"
                    "  baseline_rate=200/min  deficit={hlr_deficit}/min\n"
                    "  esim_affected=true  physical_sim_affected=true\n"
                    "ACTION: scale_hlr_workers=true  drain_queue=true  alert=PROV-SIM-STALL"
                ),
                "infrastructure_events": [
                    {
                        "body": "[GCP-Monitoring] HLRProvisioningStall: queue_depth=5000 processing_rate=0/min hlr_connection=TIMEOUT target=subscriber-db region=europe-west3",
                        "severity": "ERROR",
                        "event_name": "gcp.monitoring.HLRProvisioningStall",
                        "service": "provisioning-service",
                        "attributes": {
                            "cloud.provider": "gcp",
                            "cloud.service": "monitoring",
                            "gcp.resource_type": "gke_container",
                            "infrastructure.source": "GCP-Monitoring",
                        },
                    },
                ],
            },
            11: {
                "name": "Number Portability Lookup Timeout",
                "subsystem": "provisioning",
                "vehicle_section": "number_mgmt",
                "error_type": "PROV-NP-TIMEOUT",
                "sensor_type": "number_port",
                "affected_services": ["provisioning-service"],
                "cascade_services": ["activation-service"],
                "description": "MNP (Mobile Number Portability) database query timeout delaying ported number activations",
                "investigation_notes": (
                    "1. Check MNP central database connectivity — the national CRDB (Central Reference Database) timeout "
                    "at 5000ms indicates the external MNP service is degraded. This is outside Meridian's control.\n"
                    "2. MNP lookups are required for all ported numbers to verify the current operator. Without CRDB response, "
                    "provisioning-service cannot determine if the number is eligible for activation.\n"
                    "3. Non-ported numbers (new MSISDN allocations) are NOT affected — only customers porting from another "
                    "carrier experience delays. Approximately 15% of activations involve number portability.\n"
                    "4. Enable MNP cache fallback — provisioning-service maintains a 24-hour cache of recent MNP lookups. "
                    "For repeat lookups (e.g., retry after initial failure), cached results can be served.\n"
                    "5. Contact CRDB operations center for status update. If prolonged outage, consider accepting activations "
                    "without MNP verification and reconciling asynchronously (regulatory risk — requires approval)."
                ),
                "remediation_action": "enable_mnp_cache_fallback",
                "error_message": "[PROV] PROV-NP-TIMEOUT: msisdn={msisdn} donor_operator={donor_operator} crdb_timeout_ms={crdb_timeout_ms} cache_hit={mnp_cache_hit} port_type={port_type}",
                "stack_trace": (
                    "=== MNP LOOKUP FAILURE ===\n"
                    "msisdn={msisdn}  port_type={port_type}\n"
                    "--- PORTABILITY CHECK ---\n"
                    "  STEP                    STATUS      ELAPSED_MS\n"
                    "  msisdn_format_check     COMPLETE    5\n"
                    "  crdb_query              TIMEOUT     {crdb_timeout_ms}  <<< EXTERNAL\n"
                    "  donor_verification      SKIPPED     ---\n"
                    "  port_eligibility        SKIPPED     ---\n"
                    "--- CRDB STATUS ---\n"
                    "  endpoint=crdb.telecom-regulator.eu  response=TIMEOUT\n"
                    "  donor_operator={donor_operator}  cache_hit={mnp_cache_hit}\n"
                    "  ported_activations_affected=15%  new_msisdn_unaffected=true\n"
                    "ACTION: enable_cache_fallback=true  contact_crdb_ops=true  alert=PROV-NP-TIMEOUT"
                ),
                "infrastructure_events": [
                    {
                        "body": "[GCP-Monitoring] NumberPortabilityTimeout: npdb_endpoint=npdb.meridian.eu response_time_ms=12000 threshold_ms=3000 region=europe-west3",
                        "severity": "WARN",
                        "event_name": "gcp.monitoring.NumberPortabilityTimeout",
                        "service": "provisioning-service",
                        "attributes": {
                            "cloud.provider": "gcp",
                            "cloud.service": "monitoring",
                            "gcp.resource_type": "gke_container",
                            "infrastructure.source": "GCP-Monitoring",
                        },
                    },
                ],
            },
            12: {
                "name": "Tariff Plan Mismatch",
                "subsystem": "provisioning",
                "vehicle_section": "tariff_engine",
                "error_type": "PROV-TARIFF-MISMATCH",
                "sensor_type": "tariff_validation",
                "affected_services": ["provisioning-service", "bss-billing"],
                "cascade_services": ["activation-service"],
                "description": "Tariff plan code mismatch between provisioning-service and bss-billing causing activation rejection",
                "investigation_notes": (
                    "1. Check tariff plan code mapping — provisioning-service uses internal plan codes (e.g., 5G-PLUS-EU-v3) "
                    "while bss-billing uses BSS product codes (e.g., PROD-5G-2024-PLUS). Mapping table sync failure causes mismatches.\n"
                    "2. Recent tariff plan update (deployment 2026-03-05) added new 5G Max tier but the BSS mapping was not "
                    "updated. All 5G Max activations fail with PLAN_CODE_NOT_FOUND on bss-billing side.\n"
                    "3. Affected plans: check which plan codes return MISMATCH — if limited to new plans, hotfix the mapping "
                    "table. If widespread, rollback the tariff plan deployment.\n"
                    "4. BSS-billing validates plan code before creating billing account — rejection at this stage means the "
                    "subscriber is activated on the network but has no billing record. Requires manual reconciliation.\n"
                    "5. Query orphaned activations: SELECT msisdn, plan_code FROM activation_log WHERE billing_status='REJECTED' "
                    "AND ts > NOW() - INTERVAL 1 HOUR. These subscribers need manual billing account creation."
                ),
                "remediation_action": "sync_tariff_mapping",
                "error_message": "[PROV] PROV-TARIFF-MISMATCH: plan_code={plan_code} bss_code={bss_code} error={tariff_error} msisdn={msisdn} tier={subscription_tier}",
                "stack_trace": (
                    "=== TARIFF PLAN VALIDATION ===\n"
                    "msisdn={msisdn}  tier={subscription_tier}\n"
                    "--- PLAN MAPPING ---\n"
                    "  provisioning_code={plan_code}\n"
                    "  bss_product_code={bss_code}\n"
                    "  mapping_status=NOT_FOUND  <<< MISMATCH\n"
                    "--- VALIDATION PIPELINE ---\n"
                    "  STEP                    STATUS      DETAIL\n"
                    "  plan_code_lookup        COMPLETE    {plan_code}\n"
                    "  bss_mapping_resolve     FAILED      {tariff_error}\n"
                    "  billing_account_create  SKIPPED     ---\n"
                    "  activation_confirm      SKIPPED     ---\n"
                    "--- IMPACT ---\n"
                    "  orphaned_activations={orphaned_count}  requires_manual_reconciliation=true\n"
                    "  affected_tiers=[5G Max, eSIM Premium]  last_tariff_deploy=2026-03-05\n"
                    "ACTION: hotfix_mapping=true  reconcile_orphans=true  alert=PROV-TARIFF-MISMATCH"
                ),
                "infrastructure_events": [
                    {
                        "body": "[CloudWatch] TariffPlanSyncFailure: source=provisioning-service target=bss-billing mismatch_count=150 last_sync=45min_ago region=eu-central-1",
                        "severity": "WARN",
                        "event_name": "aws.cloudwatch.alarm.TariffPlanSyncFailure",
                        "service": "bss-billing",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "monitoring",
                            "infrastructure.source": "AzureMonitor",
                        },
                    },
                ],
            },
            13: {
                "name": "CDR Processing Backlog",
                "subsystem": "billing",
                "vehicle_section": "cdr_pipeline",
                "error_type": "BSS-CDR-BACKLOG",
                "sensor_type": "cdr_processing",
                "affected_services": ["bss-billing", "subscriber-db"],
                "cascade_services": ["notification-service"],
                "description": "Call Detail Record mediation pipeline backlogged causing delayed billing and usage reporting",
                "investigation_notes": (
                    "1. Check CDR ingestion vs processing rate — ingestion at {cdr_ingestion_rate}/min with processing at "
                    "{cdr_processing_rate}/min creates a growing deficit. Backlog grows by ~{cdr_deficit}/min.\n"
                    "2. CDR mediation involves: raw CDR ingestion → format normalization → rating → account posting → "
                    "invoice generation. Identify which stage is the bottleneck.\n"
                    "3. Rating engine performance — complex tariff plans (Enterprise multi-APN, MVNO wholesale) take 10x "
                    "longer to rate than Consumer plans. Check rating engine CPU utilization.\n"
                    "4. Subscriber-db writes for balance updates are queuing — CDR processing writes real-time usage data "
                    "to subscriber profiles. Large backlog causes stale usage data visible to customers.\n"
                    "5. Enterprise SLA reporting depends on timely CDR processing — delays beyond 30min breach reporting "
                    "SLAs. Notify enterprise account managers if backlog exceeds 15min."
                ),
                "remediation_action": "scale_cdr_workers",
                "error_message": "[BSS] BSS-CDR-BACKLOG: queue_depth={cdr_queue_depth} ingestion_rate={cdr_ingestion_rate}/min processing_rate={cdr_processing_rate}/min oldest_cdr_min={cdr_oldest_min} backlog_gb={cdr_backlog_gb}",
                "stack_trace": (
                    "=== CDR PROCESSING PIPELINE ===\n"
                    "pipeline=meridian-cdr-mediation  mode=real-time\n"
                    "--- PIPELINE STAGES ---\n"
                    "  STAGE                STATUS      RATE        QUEUE\n"
                    "  raw_ingestion        OK          {cdr_ingestion_rate}/min   ---\n"
                    "  format_normalize     OK          {cdr_ingestion_rate}/min   200\n"
                    "  rating_engine        BACKLOGGED  {cdr_processing_rate}/min  {cdr_queue_depth}  <<< BOTTLENECK\n"
                    "  account_posting      WAITING     ---         ---\n"
                    "  invoice_generation   WAITING     ---         ---\n"
                    "--- BACKLOG ANALYSIS ---\n"
                    "  deficit={cdr_deficit}/min  backlog_size={cdr_backlog_gb}GB\n"
                    "  oldest_unprocessed={cdr_oldest_min}min  sla_threshold=30min\n"
                    "  enterprise_reporting=DELAYED  mvno_wholesale=DELAYED\n"
                    "ACTION: scale_rating_workers=true  notify_enterprise=true  alert=BSS-CDR-BACKLOG"
                ),
                "infrastructure_events": [
                    {
                        "body": "[AzureMonitor] CDRProcessingBacklog: queue_depth=50000 processing_rate=200/min normal_rate=5000/min mediation_pipeline=STALLED region=westeurope",
                        "severity": "ERROR",
                        "event_name": "azure.monitor.CDRProcessingBacklog",
                        "service": "bss-billing",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "monitor",
                            "azure.health.status": "Critical",
                            "infrastructure.source": "AzureMonitor",
                        },
                    },
                ],
            },
            14: {
                "name": "Billing Mediation Failure",
                "subsystem": "billing",
                "vehicle_section": "mediation",
                "error_type": "BSS-MEDIATION-FAIL",
                "sensor_type": "billing_mediation",
                "affected_services": ["bss-billing"],
                "cascade_services": ["activation-service", "provisioning-service"],
                "description": "Billing mediation node failure causing CDR deduplication errors and incorrect charge application",
                "investigation_notes": (
                    "1. Check mediation node health — node failure causes CDR routing to surviving nodes, which may lack "
                    "the deduplication state for in-flight CDRs. This leads to duplicate charges.\n"
                    "2. CDR deduplication uses a 24-hour sliding window with SHA-256 hash of (MSISDN, timestamp, duration, "
                    "APN). Node failure loses the in-memory dedup window for that node's partition.\n"
                    "3. Duplicate charge detection: SELECT msisdn, COUNT(*) FROM cdr_rated WHERE ts > NOW() - INTERVAL 1 HOUR "
                    "GROUP BY msisdn, event_hash HAVING COUNT(*) > 1. Flag these for reversal.\n"
                    "4. Provisioning-service and activation-service are impacted because new activations require a billing "
                    "validation step — if mediation is unhealthy, the validation returns errors.\n"
                    "5. Initiate CDR replay for the failed node's partition after recovery — replay from the last checkpoint "
                    "ensures no CDRs are lost. Monitor for duplicates during replay."
                ),
                "remediation_action": "failover_mediation_node",
                "error_message": "[BSS] BSS-MEDIATION-FAIL: node={mediation_node} partition={mediation_partition} duplicate_cdrs={duplicate_cdr_count} checkpoint_age_min={checkpoint_age_min}",
                "stack_trace": (
                    "=== BILLING MEDIATION STATUS ===\n"
                    "cluster=meridian-mediation  mode=active-active\n"
                    "--- NODE STATUS ---\n"
                    "  NODE              STATUS      PARTITION    LAST_CHECKPOINT\n"
                    "  mediation-01      OK          P0,P1        2min ago\n"
                    "  mediation-02      FAILED      P2,P3        {checkpoint_age_min}min ago  <<< DOWN\n"
                    "  mediation-03      OK          P4,P5        1min ago\n"
                    "--- IMPACT ---\n"
                    "  dedup_window_lost=true  partition={mediation_partition}\n"
                    "  duplicate_cdrs_detected={duplicate_cdr_count}\n"
                    "  cdrs_at_risk={cdrs_at_risk}  replay_required=true\n"
                    "  billing_validation=DEGRADED  new_activations=DELAYED\n"
                    "ACTION: failover_partition=true  replay_from_checkpoint=true  alert=BSS-MEDIATION-FAIL"
                ),
                "infrastructure_events": [
                    {
                        "body": "[AzureMonitor] BillingMediationFailure: pipeline=cdr-mediation stage=rating error_rate=40% revenue_impact=EUR_45000/hr region=westeurope",
                        "severity": "ERROR",
                        "event_name": "azure.monitor.BillingMediationFailure",
                        "service": "bss-billing",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "monitor",
                            "azure.health.status": "Critical",
                            "infrastructure.source": "AzureMonitor",
                        },
                    },
                ],
            },
            15: {
                "name": "Enterprise SLA Breach Alert",
                "subsystem": "billing",
                "vehicle_section": "sla_monitor",
                "error_type": "BSS-SLA-BREACH",
                "sensor_type": "sla_monitoring",
                "affected_services": ["bss-billing", "notification-service"],
                "cascade_services": ["api-gateway"],
                "description": "Enterprise customer SLA thresholds breached requiring escalation and financial credit calculation",
                "investigation_notes": (
                    "1. Check SLA breach severity — Enterprise SLA: 99.5% activation success, max 30s activation time. "
                    "Current success rate {sla_success_rate}% with avg activation time {sla_activation_time_s}s.\n"
                    "2. SLA breach duration determines financial credit: <15min = warning only, 15-30min = 5% monthly credit, "
                    "30-60min = 10% credit, >60min = 20% credit + executive escalation.\n"
                    "3. MVNO partner SLA: 99.0% activation success. MVNO breaches trigger wholesale revenue credits and "
                    "potential partnership review. Check MVNO-specific metrics separately.\n"
                    "4. Notification-service must send breach alerts to: enterprise account managers, MVNO partner liaisons, "
                    "VP of Enterprise Sales, and SRE on-call. Verify notification delivery.\n"
                    "5. Begin RCA documentation immediately — enterprise contract requires RCA delivery within 48 hours of "
                    "SLA breach. Include: timeline, root cause, customer impact, remediation actions, prevention plan."
                ),
                "remediation_action": "escalate_sla_breach",
                "error_message": "[BSS] BSS-SLA-BREACH: customer_segment={sla_segment} success_rate={sla_success_rate}% sla_target={sla_target}% breach_duration_min={sla_breach_min} credit_tier={sla_credit_tier}",
                "stack_trace": (
                    "=== SLA BREACH REPORT ===\n"
                    "segment={sla_segment}  contract=ENT-2024-MERIDIAN\n"
                    "--- SLA METRICS ---\n"
                    "  METRIC                  CURRENT     SLA_TARGET   STATUS\n"
                    "  activation_success      {sla_success_rate}%       {sla_target}%       BREACH\n"
                    "  activation_time_p99     {sla_activation_time_s}s        30s          BREACH\n"
                    "  availability            99.1%       99.5%        BREACH\n"
                    "--- BREACH TIMELINE ---\n"
                    "  breach_start={breach_start_time}  duration={sla_breach_min}min\n"
                    "  credit_tier={sla_credit_tier}  estimated_credit=${sla_credit_amount}\n"
                    "--- ESCALATION ---\n"
                    "  enterprise_accounts_affected={enterprise_affected}\n"
                    "  mvno_partners_affected={mvno_affected}\n"
                    "  rca_due_date=48h_from_breach  notifications_sent={notifications_sent}\n"
                    "ACTION: escalate_enterprise=true  calculate_credits=true  alert=BSS-SLA-BREACH"
                ),
                "infrastructure_events": [
                    {
                        "body": "[AzureMonitor] EnterpriseSLABreach: customer_segment=Enterprise activation_success_rate=89.2% sla_threshold=99.5% penalty_risk=EUR_250000 region=westeurope",
                        "severity": "ERROR",
                        "event_name": "azure.monitor.EnterpriseSLABreach",
                        "service": "bss-billing",
                        "attributes": {
                            "cloud.provider": "azure",
                            "cloud.service": "monitor",
                            "azure.health.status": "Critical",
                            "infrastructure.source": "AzureMonitor",
                        },
                    },
                ],
            },
            16: {
                "name": "API Rate Limit Cascade",
                "subsystem": "gateway",
                "vehicle_section": "rate_limiter",
                "error_type": "GW-RATE-LIMIT-CASCADE",
                "sensor_type": "rate_limiting",
                "affected_services": ["api-gateway"],
                "cascade_services": ["activation-service", "auth-service"],
                "description": "API gateway rate limiter triggering cascade rejection due to retry storm from downstream failures",
                "investigation_notes": (
                    "1. Check rate limiter token bucket state — bucket at 0 tokens with refill rate 1000/min means all "
                    "incoming requests are rejected with 429 Too Many Requests.\n"
                    "2. Root cause is typically a downstream failure (DB, network, auth) causing clients to retry aggressively. "
                    "The retry storm amplifies traffic 3-5x above baseline, exhausting rate limit budgets.\n"
                    "3. Identify the retry source — check api-gateway access logs for client IP distribution. A small number "
                    "of IPs generating >100 req/min indicates aggressive retry without backoff.\n"
                    "4. Enable adaptive rate limiting: increase burst allowance to 2000/min while maintaining sustained rate "
                    "at 1000/min. This absorbs retry bursts without permanently raising limits.\n"
                    "5. Add Retry-After header to 429 responses with exponential backoff suggestion (1s, 2s, 4s, 8s). "
                    "Well-behaved clients (mobile apps) should honor this header."
                ),
                "remediation_action": "adjust_rate_limits",
                "error_message": "[GW] GW-RATE-LIMIT-CASCADE: current_rate={gw_current_rate}/min limit={gw_rate_limit}/min rejected={gw_rejected_count} burst_factor={gw_burst_factor}x source={gw_source}",
                "stack_trace": (
                    "=== API GATEWAY RATE LIMITER ===\n"
                    "gateway=api-gateway  algorithm=token_bucket\n"
                    "--- RATE LIMITER STATUS ---\n"
                    "  METRIC              VALUE       LIMIT       STATUS\n"
                    "  requests/min        {gw_current_rate}       {gw_rate_limit}       EXCEEDED\n"
                    "  burst_tokens        0           200         DEPLETED\n"
                    "  rejected_count      {gw_rejected_count}       ---         ---\n"
                    "  retry_amplification {gw_burst_factor}x       ---         STORM\n"
                    "--- SOURCE ANALYSIS ---\n"
                    "  source={gw_source}  unique_clients={gw_unique_clients}\n"
                    "  top_client_rate=142/min  avg_client_rate=12/min\n"
                    "  retry_after_honored=23%  aggressive_retry=77%\n"
                    "ACTION: adaptive_rate_limit=true  add_retry_after=true  alert=GW-RATE-LIMIT-CASCADE"
                ),
                "infrastructure_events": [
                    {
                        "body": "[CloudWatch] APIGatewayRateLimitExceeded: stage=production endpoint=/api/v1/* throttled_requests=2500 rate_limit=1000/min burst=50 region=eu-central-1",
                        "severity": "WARN",
                        "event_name": "aws.cloudwatch.alarm.APIGatewayRateLimitExceeded",
                        "service": "api-gateway",
                        "attributes": {
                            "cloud.provider": "aws",
                            "cloud.service": "apigateway",
                            "aws.cloudwatch.alarm_name": "api-gateway-rate-limit",
                            "aws.cloudwatch.state": "ALARM",
                            "infrastructure.source": "CloudWatch",
                        },
                    },
                ],
            },
            17: {
                "name": "mTLS Certificate Expiry",
                "subsystem": "gateway",
                "vehicle_section": "tls_termination",
                "error_type": "GW-MTLS-EXPIRY",
                "sensor_type": "certificate",
                "affected_services": ["api-gateway", "auth-service"],
                "cascade_services": ["activation-service"],
                "description": "Mutual TLS certificate approaching expiry causing service-to-service authentication failures",
                "investigation_notes": (
                    "1. Check certificate expiry time — certificates with <24h remaining trigger WARNING, <4h triggers "
                    "CRITICAL. Expired certificates cause immediate TLS handshake failures on all mTLS connections.\n"
                    "2. cert-manager auto-renewal failed — check cert-manager logs: kubectl logs -n cert-manager "
                    "deploy/cert-manager. Common failure: DNS-01 challenge timeout (Route53 API permission issue).\n"
                    "3. Impact: api-gateway ↔ auth-service mTLS connection will fail when cert expires. All authentication "
                    "requests will fail, blocking all API traffic including activations.\n"
                    "4. Emergency cert issuance: use AWS ACM for api-gateway or Azure Key Vault for auth-service. "
                    "Manual cert installation: kubectl create secret tls meridian-mtls --cert=cert.pem --key=key.pem.\n"
                    "5. Verify full certificate chain including intermediate CA. Self-signed intermediates must be trusted "
                    "by both api-gateway and auth-service trust stores."
                ),
                "remediation_action": "emergency_cert_renewal",
                "error_message": "[GW] GW-MTLS-EXPIRY: domain={cert_domain} expires_in={cert_hours_left}h serial={cert_serial} issuer={cert_issuer} services_affected={cert_svc_count}",
                "stack_trace": (
                    "=== CERTIFICATE STATUS REPORT ===\n"
                    "domain={cert_domain}  issuer={cert_issuer}\n"
                    "--- CERTIFICATE DETAILS ---\n"
                    "  serial={cert_serial}\n"
                    "  not_after=2026-03-09T23:59:59Z\n"
                    "  remaining={cert_hours_left}h  <<< EXPIRING\n"
                    "  subject=CN={cert_domain}\n"
                    "  san={cert_domain},*.{cert_domain}\n"
                    "--- SERVICE IMPACT ---\n"
                    "  services_affected={cert_svc_count}\n"
                    "  SERVICE              TLS_STATUS    HANDSHAKE\n"
                    "  api-gateway          WARNING       last_success=2min_ago\n"
                    "  auth-service         WARNING       last_success=2min_ago\n"
                    "  activation-service   DEGRADED      upstream_tls_risk\n"
                    "--- AUTO-RENEWAL ---\n"
                    "  cert_manager=FAILED  last_attempt=2h_ago  error=DNS_CHALLENGE_TIMEOUT\n"
                    "ACTION: emergency_cert_issue=true  manual_install=true  alert=GW-MTLS-EXPIRY"
                ),
                "infrastructure_events": [
                    {
                        "body": "[cert-manager] Certificate renewal FAILED: domain=api.meridian-telecom.eu issuer=letsencrypt-prod error=DNS-01 challenge timeout (Route53 API rate limited) last_attempt=2h_ago next_retry=1h",
                        "severity": "ERROR",
                        "event_name": "cert-manager.renewal.failed",
                        "service": "api-gateway",
                        "attributes": {
                            "k8s.namespace": "cert-manager",
                            "k8s.component": "cert-manager",
                            "cert.domain": "api.meridian-telecom.eu",
                            "cert.issuer": "letsencrypt-prod",
                            "cert.error": "DNS-01 challenge timeout",
                            "infrastructure.source": "cert-manager",
                        },
                    },
                ],
            },
            18: {
                "name": "SMS Gateway Delivery Failure",
                "subsystem": "notifications",
                "vehicle_section": "sms_channel",
                "error_type": "NOTIF-SMS-FAIL",
                "sensor_type": "sms_delivery",
                "affected_services": ["notification-service"],
                "cascade_services": ["activation-service"],
                "description": "SMPP gateway connection failure preventing activation confirmation SMS delivery to subscribers",
                "investigation_notes": (
                    "1. Check SMPP connection status — SMPP bind to SMS aggregator failed or connection dropped. "
                    "Verify: notification-service metrics for smpp.connection_status and smpp.bind_state.\n"
                    "2. SMS delivery is required for activation confirmation — subscribers expect an SMS within 60s of "
                    "successful activation. Delayed SMS causes customer confusion and support calls.\n"
                    "3. SMPP aggregator may be throttling — check for ESME_RTHROTTLED responses. Meridian's contracted "
                    "throughput is 500 SMS/min. Verify current send rate against contract.\n"
                    "4. Enable fallback delivery channels: push notification via mobile app, email notification. "
                    "For Enterprise customers, also trigger webhook notification to their systems.\n"
                    "5. OTP/2FA SMS are CRITICAL — if SMS is used for authentication OTP delivery, auth-service must "
                    "fall back to email OTP or TOTP. Check auth-service MFA configuration."
                ),
                "remediation_action": "reconnect_smpp_gateway",
                "error_message": "[NOTIF] NOTIF-SMS-FAIL: smpp_status={smpp_status} delivery_rate={sms_delivery_rate}% queued={sms_queued} aggregator={sms_aggregator} error={sms_error}",
                "stack_trace": (
                    "=== SMS GATEWAY STATUS ===\n"
                    "aggregator={sms_aggregator}  protocol=SMPPv3.4\n"
                    "--- CONNECTION STATUS ---\n"
                    "  bind_state={smpp_status}  last_bind_attempt=45s_ago\n"
                    "  error={sms_error}\n"
                    "  contracted_throughput=500/min  current_rate=0/min\n"
                    "--- DELIVERY METRICS ---\n"
                    "  METRIC              VALUE       TARGET      STATUS\n"
                    "  delivery_rate       {sms_delivery_rate}%       98%         DEGRADED\n"
                    "  queued_messages     {sms_queued}       100         OVERFLOW\n"
                    "  failed_deliveries   {sms_failed}       0           CRITICAL\n"
                    "  activation_confirms_pending={activation_confirms_pending}\n"
                    "--- FALLBACK ---\n"
                    "  push_notification=AVAILABLE  email=AVAILABLE\n"
                    "  otp_delivery=BLOCKED  auth_impact=HIGH\n"
                    "ACTION: reconnect_smpp=true  enable_fallback=true  alert=NOTIF-SMS-FAIL"
                ),
                "infrastructure_events": [
                    {
                        "body": "[GCP-Monitoring] SMPPConnectionLost: aggregator=twilio-smpp bind_state=UNBOUND last_bind=45s_ago error=CONNECTION_REFUSED region=europe-west3",
                        "severity": "ERROR",
                        "event_name": "gcp.monitoring.SMPPConnectionLost",
                        "service": "notification-service",
                        "attributes": {
                            "cloud.provider": "gcp",
                            "cloud.service": "monitoring",
                            "gcp.resource_type": "gke_container",
                            "infrastructure.source": "GCP-Monitoring",
                        },
                    },
                ],
            },
            19: {
                "name": "Push Notification Backpressure",
                "subsystem": "notifications",
                "vehicle_section": "push_channel",
                "error_type": "NOTIF-PUSH-BACKPRESSURE",
                "sensor_type": "push_delivery",
                "affected_services": ["notification-service"],
                "cascade_services": ["activation-service", "provisioning-service"],
                "description": "FCM/APNs push notification delivery backpressure causing activation confirmation delays",
                "investigation_notes": (
                    "1. Check FCM/APNs rate limit status — Firebase Cloud Messaging and Apple Push Notification service "
                    "have per-app rate limits. Exceeding limits causes 429 responses and message queuing.\n"
                    "2. Backpressure source: activation volume spike (e.g., marketing campaign, device launch event) "
                    "driving notification volume above normal 200/min to {push_current_rate}/min.\n"
                    "3. APNs priority channels: activation confirmations should use priority=10 (immediate). Background "
                    "notifications (usage alerts, marketing) should use priority=5 to avoid quota competition.\n"
                    "4. Provisioning-service waits for notification-service acknowledgment before marking activation complete. "
                    "Push backpressure delays this acknowledgment, causing provisioning timeout.\n"
                    "5. Decouple notification from activation flow — make notification async so provisioning-service doesn't "
                    "block on notification delivery. Queue notifications for background delivery."
                ),
                "remediation_action": "throttle_push_notifications",
                "error_message": "[NOTIF] NOTIF-PUSH-BACKPRESSURE: platform={push_platform} rate={push_current_rate}/min limit={push_rate_limit}/min queue_depth={push_queue_depth} retry_backoff_ms={push_backoff_ms}",
                "stack_trace": (
                    "=== PUSH NOTIFICATION BACKPRESSURE ===\n"
                    "platform={push_platform}  service=notification-service\n"
                    "--- DELIVERY STATUS ---\n"
                    "  PLATFORM     RATE        LIMIT       QUEUE     STATUS\n"
                    "  FCM          {push_current_rate}/min  {push_rate_limit}/min  {push_queue_depth}     THROTTLED\n"
                    "  APNs         {push_current_rate}/min  {push_rate_limit}/min  {push_queue_depth}     THROTTLED\n"
                    "--- BACKPRESSURE ANALYSIS ---\n"
                    "  retry_backoff={push_backoff_ms}ms  max_retries=5\n"
                    "  activation_confirms_delayed={activation_confirms_delayed}\n"
                    "  provisioning_timeout_risk=HIGH\n"
                    "  priority_10_queued={priority_high_queued}  priority_5_queued={priority_low_queued}\n"
                    "ACTION: prioritize_activation=true  defer_marketing=true  alert=NOTIF-PUSH-BACKPRESSURE"
                ),
                "infrastructure_events": [
                    {
                        "body": "[GCP-Monitoring] PushNotificationThrottled: platform=FCM rate=800/min limit=500/min queue_depth=3500 429_responses=350 region=europe-west3",
                        "severity": "WARN",
                        "event_name": "gcp.monitoring.PushNotificationThrottled",
                        "service": "notification-service",
                        "attributes": {
                            "cloud.provider": "gcp",
                            "cloud.service": "monitoring",
                            "gcp.resource_type": "gke_container",
                            "infrastructure.source": "GCP-Monitoring",
                        },
                    },
                ],
            },
            20: {
                "name": "MVNO Webhook Timeout",
                "subsystem": "notifications",
                "vehicle_section": "partner_integration",
                "error_type": "NOTIF-MVNO-WEBHOOK-TIMEOUT",
                "sensor_type": "webhook",
                "affected_services": ["notification-service", "api-gateway"],
                "cascade_services": ["bss-billing"],
                "description": "MVNO partner webhook endpoints timing out preventing activation status callbacks",
                "investigation_notes": (
                    "1. Check MVNO webhook endpoint health — partner webhook at {webhook_url} returning timeout after "
                    "{webhook_timeout_ms}ms. This is the partner's infrastructure, not Meridian's.\n"
                    "2. MVNO partners (Lycamobile, Lebara, etc.) depend on webhook callbacks for activation confirmations. "
                    "Without callbacks, MVNO subscribers show 'activation pending' indefinitely.\n"
                    "3. Webhook retry policy: 3 retries with exponential backoff (5s, 30s, 300s). After 3 failures, "
                    "notification is queued for manual reconciliation.\n"
                    "4. Enable webhook circuit breaker — if a partner endpoint fails >50% of calls in a 5-minute window, "
                    "stop sending to avoid overwhelming their recovering system.\n"
                    "5. BSS-billing is affected because MVNO wholesale billing reconciliation depends on successful webhook "
                    "delivery confirmations. Unacknowledged activations cannot be billed to the MVNO partner."
                ),
                "remediation_action": "enable_webhook_circuit_breaker",
                "error_message": "[NOTIF] NOTIF-MVNO-WEBHOOK-TIMEOUT: partner={mvno_partner} endpoint={webhook_endpoint} timeout_ms={webhook_timeout_ms} failed={webhook_failures} pending={webhook_pending}",
                "stack_trace": (
                    "=== MVNO WEBHOOK STATUS ===\n"
                    "partner={mvno_partner}  integration=REST_WEBHOOK\n"
                    "--- ENDPOINT STATUS ---\n"
                    "  endpoint={webhook_endpoint}\n"
                    "  timeout_ms={webhook_timeout_ms}  sla_ms=5000\n"
                    "  success_rate={webhook_success_rate}%  threshold=95%\n"
                    "--- DELIVERY METRICS ---\n"
                    "  ATTEMPT     STATUS      ELAPSED\n"
                    "  1/3         TIMEOUT     {webhook_timeout_ms}ms\n"
                    "  2/3         TIMEOUT     {webhook_timeout_ms}ms\n"
                    "  3/3         TIMEOUT     {webhook_timeout_ms}ms\n"
                    "  circuit_breaker=OPEN  cooldown=300s\n"
                    "--- BILLING IMPACT ---\n"
                    "  unacknowledged_activations={webhook_pending}\n"
                    "  wholesale_billing=BLOCKED  reconciliation=MANUAL_REQUIRED\n"
                    "  partner_sla_breach_risk=HIGH\n"
                    "ACTION: circuit_breaker=true  queue_for_reconciliation=true  alert=NOTIF-MVNO-WEBHOOK-TIMEOUT"
                ),
                "infrastructure_events": [
                    {
                        "body": "[CloudWatch] MVNOWebhookTimeout: partner=Lycamobile endpoint=api.lycamobile.eu/webhooks/activate timeout_ms=15000 sla_ms=5000 circuit_breaker=OPEN region=eu-central-1",
                        "severity": "ERROR",
                        "event_name": "aws.cloudwatch.alarm.MVNOWebhookTimeout",
                        "service": "api-gateway",
                        "attributes": {
                            "cloud.provider": "aws",
                            "cloud.service": "cloudwatch",
                            "aws.cloudwatch.alarm_name": "mvno-webhook-health",
                            "aws.cloudwatch.state": "ALARM",
                            "infrastructure.source": "CloudWatch",
                        },
                    },
                ],
            },
        }

    # ── Topology ──────────────────────────────────────────────────────

    @property
    def service_topology(self) -> dict[str, list[tuple[str, str, str]]]:
        return {
            "api-gateway": [
                ("activation-service", "/api/v1/activate", "POST"),
                ("auth-service", "/api/v1/auth/validate", "POST"),
            ],
            "activation-service": [
                ("provisioning-service", "/api/v1/provision", "POST"),
                ("auth-service", "/api/v1/auth/token", "POST"),
                ("notification-service", "/api/v1/notify/activation", "POST"),
            ],
            "provisioning-service": [
                ("subscriber-db", "/api/v1/db/subscribers/query", "POST"),
                ("network-core", "/api/v1/network/allocate", "POST"),
                ("notification-service", "/api/v1/notify/provisioning", "POST"),
            ],
            "network-core": [
                ("bss-billing", "/api/v1/billing/activate", "POST"),
            ],
            "auth-service": [
                ("identity-db", "/api/v1/db/identities/query", "POST"),
            ],
            "bss-billing": [
                ("subscriber-db", "/api/v1/db/subscribers/update", "POST"),
            ],
            "notification-service": [
                ("subscriber-db", "/api/v1/db/notifications/insert", "POST"),
            ],
        }

    @property
    def entry_endpoints(self) -> dict[str, list[tuple[str, str]]]:
        # Only services that receive external traffic should be entry points.
        # Leaf services (subscriber-db, identity-db, notification-service) appear
        # only as downstream callees — keeping them out prevents disconnected
        # nodes in the APM Service Map.
        return {
            "api-gateway": [
                ("/api/v1/activate", "POST"),
                ("/api/v1/bundle/5g-home", "GET"),
                ("/api/v1/customer/status", "GET"),
            ],
            "activation-service": [("/api/v1/activation/status", "GET")],
            "provisioning-service": [("/api/v1/provision/status", "GET")],
            "network-core": [("/api/v1/network/status", "GET")],
            "auth-service": [("/api/v1/auth/health", "GET")],
            "bss-billing": [("/api/v1/billing/status", "GET")],
        }

    @property
    def db_operations(self) -> dict[str, list[tuple[str, str, str]]]:
        return {
            "provisioning-service": [
                ("SELECT", "subscribers", "SELECT sim_profile, tariff_plan FROM subscribers WHERE msisdn = $1"),
                ("UPDATE", "subscribers", "UPDATE subscribers SET status = 'provisioning', updated_at = NOW() WHERE msisdn = $1"),
                ("INSERT", "activation_log", "INSERT INTO activation_log (msisdn, event, ts) VALUES ($1, $2, NOW())"),
            ],
            "auth-service": [
                ("SELECT", "identities", "SELECT user_id, role, last_auth FROM identities WHERE token_hash = $1"),
                ("SELECT", "sessions", "SELECT session_id, expires_at FROM sessions WHERE user_id = $1 AND active = true"),
            ],
            "bss-billing": [
                ("SELECT", "billing_accounts", "SELECT account_id, balance, plan_tier FROM billing_accounts WHERE msisdn = $1"),
                ("INSERT", "billing_events", "INSERT INTO billing_events (msisdn, event_type, amount, ts) VALUES ($1, $2, $3, NOW())"),
            ],
            "subscriber-db": [
                ("SELECT", "subscriber_profiles", "SELECT msisdn, segment, subscription_tier, sim_iccid FROM subscriber_profiles WHERE msisdn = $1"),
            ],
            "notification-service": [
                ("INSERT", "notification_log", "INSERT INTO notification_log (msisdn, channel, template, status, ts) VALUES ($1, $2, $3, $4, NOW())"),
            ],
        }

    # ── Infrastructure ────────────────────────────────────────────────

    @property
    def hosts(self) -> list[dict[str, Any]]:
        return [
            {
                "host.name": "meridian-aws-host-01",
                "host.id": "i-0e1f2a3b4c5d67890",
                "host.arch": "amd64",
                "host.type": "m5.2xlarge",
                "host.image.id": "ami-0abcdef1234567890",
                "host.cpu.model.name": "Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz",
                "host.cpu.vendor.id": "GenuineIntel",
                "host.cpu.family": "6",
                "host.cpu.model.id": "85",
                "host.cpu.stepping": "7",
                "host.cpu.cache.l2.size": 1310720,
                "host.ip": ["10.0.4.50", "172.16.3.10"],
                "host.mac": ["0a:4b:5c:6d:7e:8f", "0a:4b:5c:6d:7e:90"],
                "os.type": "linux",
                "os.description": "Amazon Linux 2023.6.20250115",
                "cloud.provider": "aws",
                "cloud.platform": "aws_ec2",
                "cloud.region": "eu-central-1",
                "cloud.availability_zone": "eu-central-1a",
                "cloud.account.id": "223344556677",
                "cloud.instance.id": "i-0e1f2a3b4c5d67890",
                "cpu_count": 8,
                "memory_total_bytes": 32 * 1024 * 1024 * 1024,
                "disk_total_bytes": 500 * 1024 * 1024 * 1024,
            },
            {
                "host.name": "meridian-gcp-host-01",
                "host.id": "5678901234567890123",
                "host.arch": "amd64",
                "host.type": "n2-standard-8",
                "host.image.id": "projects/debian-cloud/global/images/debian-12-bookworm-v20250115",
                "host.cpu.model.name": "Intel(R) Xeon(R) CPU @ 2.80GHz",
                "host.cpu.vendor.id": "GenuineIntel",
                "host.cpu.family": "6",
                "host.cpu.model.id": "85",
                "host.cpu.stepping": "7",
                "host.cpu.cache.l2.size": 1048576,
                "host.ip": ["10.128.3.40", "10.128.3.41"],
                "host.mac": ["42:01:0a:80:03:28", "42:01:0a:80:03:29"],
                "os.type": "linux",
                "os.description": "Debian GNU/Linux 12 (bookworm)",
                "cloud.provider": "gcp",
                "cloud.platform": "gcp_compute_engine",
                "cloud.region": "europe-west3",
                "cloud.availability_zone": "europe-west3-a",
                "cloud.account.id": "meridian-telecom-prod",
                "cloud.instance.id": "5678901234567890123",
                "cpu_count": 8,
                "memory_total_bytes": 32 * 1024 * 1024 * 1024,
                "disk_total_bytes": 256 * 1024 * 1024 * 1024,
            },
            {
                "host.name": "meridian-azure-host-01",
                "host.id": "/subscriptions/def-456/resourceGroups/meridian-rg/providers/Microsoft.Compute/virtualMachines/meridian-vm-01",
                "host.arch": "amd64",
                "host.type": "Standard_D4s_v3",
                "host.image.id": "Canonical:0001-com-ubuntu-server-jammy:22_04-lts-gen2:latest",
                "host.cpu.model.name": "Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz",
                "host.cpu.vendor.id": "GenuineIntel",
                "host.cpu.family": "6",
                "host.cpu.model.id": "106",
                "host.cpu.stepping": "6",
                "host.cpu.cache.l2.size": 1310720,
                "host.ip": ["10.4.0.30", "10.4.0.31"],
                "host.mac": ["00:0d:3a:8c:7d:6e", "00:0d:3a:8c:7d:6f"],
                "os.type": "linux",
                "os.description": "Ubuntu 22.04.5 LTS",
                "cloud.provider": "azure",
                "cloud.platform": "azure_vm",
                "cloud.region": "westeurope",
                "cloud.availability_zone": "westeurope-1",
                "cloud.account.id": "def-456-ghi-789",
                "cloud.instance.id": "meridian-vm-01",
                "cpu_count": 4,
                "memory_total_bytes": 16 * 1024 * 1024 * 1024,
                "disk_total_bytes": 256 * 1024 * 1024 * 1024,
            },
        ]

    @property
    def k8s_clusters(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "meridian-eks-cluster",
                "provider": "aws",
                "platform": "aws_eks",
                "region": "eu-central-1",
                "zones": ["eu-central-1a", "eu-central-1b", "eu-central-1c"],
                "os_description": "Amazon Linux 2",
                "services": ["api-gateway", "activation-service"],
            },
            {
                "name": "meridian-gke-cluster",
                "provider": "gcp",
                "platform": "gcp_gke",
                "region": "europe-west3",
                "zones": ["europe-west3-a", "europe-west3-b", "europe-west3-c"],
                "os_description": "Container-Optimized OS",
                "services": ["provisioning-service", "network-core", "notification-service"],
            },
            {
                "name": "meridian-aks-cluster",
                "provider": "azure",
                "platform": "azure_aks",
                "region": "westeurope",
                "zones": ["westeurope-1", "westeurope-2", "westeurope-3"],
                "os_description": "Ubuntu 22.04 LTS",
                "services": ["auth-service", "bss-billing"],
            },
        ]

    # ── Theme ─────────────────────────────────────────────────────────

    @property
    def theme(self) -> UITheme:
        return UITheme(
            bg_primary="#0a0e1a",
            bg_secondary="#111827",
            bg_tertiary="#1e293b",
            accent_primary="#0066FF",
            accent_secondary="#00C7B1",
            text_primary="#e2e8f0",
            text_secondary="#94a3b8",
            text_accent="#0066FF",
            status_nominal="#22c55e",
            status_warning="#eab308",
            status_critical="#ef4444",
            status_info="#3b82f6",
            font_family="'Inter', 'Segoe UI', system-ui, sans-serif",
            font_mono="'JetBrains Mono', 'Fira Code', monospace",
            chaos_title="Incident Simulator",
            service_label="Service",
            channel_label="Incident",
        )

    @property
    def apm_ml_bucket_span(self) -> str:
        """1-minute buckets so anomaly detection surfaces within ~2-3 min of
        fault onset, fitting the 10-minute live demo arc. Requires the
        trace-generator OTLP batching optimisation (send_traces_multi) to
        produce enough per-bucket data — otherwise scores are too thin."""
        return "1m"

    # ── Executive Dashboard ───────────────────────────────────────────

    @property
    def executive_kpi_emitter_service_name(self) -> str:
        return "bss-billing"

    @property
    def executive_dashboard_intro(self) -> str:
        return (
            "**Meridian Telecom 5G subscriber platform KPIs** — subscriber growth, "
            "revenue & ARPU, network quality of experience, and retention. "
            "Synthetic `business.*` OTLP gauges from `bss-billing`."
        )

    @property
    def executive_kpi_sections(self) -> list[dict]:
        return [
            {
                "header": "**Subscriber growth** — activations, MVNO, ports, eSIM",
                "specs": [
                    ("Activations / min", "metrics.business.activations_per_min"),
                    ("MVNO net-adds / min", "metrics.business.mvno_net_adds_per_min"),
                    ("Port-ins / min", "metrics.business.port_ins_per_min"),
                    ("Port-outs / min", "metrics.business.port_outs_per_min"),
                    ("eSIM provisions / min", "metrics.business.esim_provisions_per_min"),
                    ("Active subscribers (M)", "metrics.business.active_subscribers_m"),
                ],
            },
            {
                "header": "**Revenue & ARPU** — total revenue, by-tier, overage, roaming",
                "specs": [
                    ("Total revenue (USD/min)", "metrics.business.revenue_usd_per_min"),
                    ("Consumer ARPU (USD)", "metrics.business.consumer_arpu_usd"),
                    ("Enterprise ARPU (USD)", "metrics.business.enterprise_arpu_usd"),
                    ("MVNO partner revenue (USD/min)", "metrics.business.mvno_partner_revenue_usd_per_min"),
                    ("Data overage charges (USD/min)", "metrics.business.data_overage_charges_usd_per_min"),
                    ("Roaming revenue (USD/min)", "metrics.business.roaming_revenue_usd_per_min"),
                ],
            },
            {
                "header": "**Network & QoE** — handover, latency, voice, throughput",
                "specs": [
                    ("5G handover success (%)", "metrics.business.handover_success_pct"),
                    ("RAN latency p95 (ms)", "metrics.business.ran_latency_p95_ms"),
                    ("Voice MOS", "metrics.business.voice_mos"),
                    ("Data session est. success (%)", "metrics.business.data_session_success_pct"),
                    ("VoLTE call setup success (%)", "metrics.business.volte_call_setup_success_pct"),
                    ("Throughput p50 (Mbps)", "metrics.business.throughput_p50_mbps"),
                ],
            },
            {
                "header": "**Retention & support** — churn, NPS, tickets",
                "specs": [
                    ("Voluntary churn rate (%)", "metrics.business.voluntary_churn_rate_pct"),
                    ("NPS", "metrics.business.nps"),
                    ("Support tickets / min", "metrics.business.support_tickets_per_min"),
                    ("Avg ticket resolution (min)", "metrics.business.avg_ticket_resolution_min"),
                    ("First-call resolution (%)", "metrics.business.first_call_resolution_pct"),
                    ("Trouble-to-billing ratio (%)", "metrics.business.trouble_to_billing_ratio_pct"),
                ],
            },
        ]

    @property
    def executive_trend_charts(self) -> list[dict]:
        return [
            {"title": "Total revenue (USD/min)", "field": "metrics.business.revenue_usd_per_min", "y_label": "USD/min"},
            {"title": "Activations / min", "field": "metrics.business.activations_per_min", "y_label": "activations/min"},
            {"title": "Active subscribers (M)", "field": "metrics.business.active_subscribers_m", "y_label": "subscribers (M)"},
            {"title": "Voluntary churn rate (%)", "field": "metrics.business.voluntary_churn_rate_pct", "y_label": "%"},
            {"title": "RAN p95 latency (ms)", "field": "metrics.business.ran_latency_p95_ms", "y_label": "ms"},
            {"title": "NPS", "field": "metrics.business.nps", "y_label": "NPS"},
        ]

    # ── Agent Config ──────────────────────────────────────────────────

    @property
    def agent_config(self) -> dict[str, Any]:
        return {
            "id": "meridian-noc-analyst",
            "name": "5G Network Operations Analyst",
            "assessment_tool_name": "network_health_assessment",
            "system_prompt": (
                "You are the 5G Network Operations Analyst, an expert AI assistant for "
                "the Meridian Telecom 5G activation platform. You help NOC engineers investigate "
                "incidents, analyze telemetry data, and provide root cause analysis across 9 "
                "services spanning AWS Frankfurt, GCP Frankfurt, and Azure Netherlands. "
                "You have deep expertise in 5G network architecture (bearers, S1-MME, GTP tunnels), "
                "subscriber database management (45M subscribers, connection pool sizing), BSS/billing "
                "systems (CDR processing, mediation, SLA monitoring), authentication infrastructure "
                "(Azure AD federation, OAuth, session management), and notification delivery (SMS, push, "
                "MVNO webhooks). "
                "The platform serves three customer segments: Consumer (70%), Enterprise (20%), and "
                "MVNO (10%). Enterprise customers have strict SLA commitments (99.5% activation success, "
                "30s max activation time). MVNO partners (Lycamobile, Lebara) depend on webhook "
                "notifications for activation confirmations. "
                "When investigating incidents, search for these error identifiers in logs: "
                "Database faults (DB-CONNPOOL-EXHAUST, DB-RDS-MAINTENANCE, DB-REPLICATION-LAG), "
                "Network faults (NET-BEARER-TIMEOUT, NET-S1MME-CONGEST, NET-GTP-TUNNEL-FAIL), "
                "Auth faults (AUTH-TOKEN-INVALID, AUTH-FEDERATION-TIMEOUT, AUTH-SESSION-SATURATE), "
                "Provisioning faults (PROV-SIM-STALL, PROV-NP-TIMEOUT, PROV-TARIFF-MISMATCH), "
                "Billing faults (BSS-CDR-BACKLOG, BSS-MEDIATION-FAIL, BSS-SLA-BREACH), "
                "Gateway faults (GW-RATE-LIMIT-CASCADE, GW-MTLS-EXPIRY), "
                "Notification faults (NOTIF-SMS-FAIL, NOTIF-PUSH-BACKPRESSURE, NOTIF-MVNO-WEBHOOK-TIMEOUT). "
                "Log messages are in body.text — NEVER search the body field alone."
            ),
        }

    @property
    def assessment_tool_config(self) -> dict[str, Any]:
        return {
            "id": "network_health_assessment",
            "description": (
                "Comprehensive 5G platform health assessment. Evaluates all services "
                "against operational readiness criteria for subscriber activation, network "
                "core, provisioning, billing, and authentication. Returns data for impact "
                "analysis across Consumer, Enterprise, and MVNO customer segments. "
                "Log message field: body.text (never use 'body' alone)."
            ),
        }

    @property
    def knowledge_base_docs(self) -> list[dict[str, Any]]:
        return []  # Populated by deployer from channel_registry

    # ── Service Classes ───────────────────────────────────────────────

    def get_service_classes(self) -> list[type]:
        from scenarios.telecom.services.api_gateway import ApiGatewayService
        from scenarios.telecom.services.activation_service import ActivationServiceService
        from scenarios.telecom.services.provisioning_service import ProvisioningServiceService
        from scenarios.telecom.services.network_core import NetworkCoreService
        from scenarios.telecom.services.notification_service import NotificationServiceService
        from scenarios.telecom.services.auth_service import AuthServiceService
        from scenarios.telecom.services.bss_billing import BssBillingService

        # Database services (subscriber-db, identity-db) are excluded — they
        # appear only as DB dependencies on the Service Map.  Their fault logs
        # are emitted by the calling services (provisioning-service, auth-service).
        return [
            ApiGatewayService,
            ActivationServiceService,
            ProvisioningServiceService,
            NetworkCoreService,
            NotificationServiceService,
            AuthServiceService,
            BssBillingService,
        ]

    # ── Trace Attributes & RCA ───────────────────────────────────────

    def get_trace_attributes(self, service_name: str, rng) -> dict:
        # Customer segment weighted: Consumer 70%, Enterprise 20%, MVNO 10%
        segment_roll = rng.random()
        if segment_roll < 0.70:
            segment = "Consumer"
            tier = rng.choice(["5G Basic", "5G Plus", "5G Max"])
        elif segment_roll < 0.90:
            segment = "Enterprise"
            tier = rng.choice(["Business S", "Business M", "Business L"])
        else:
            segment = "MVNO"
            tier = rng.choice(["Wholesale Standard", "Wholesale Premium"])

        base = {
            "customer.segment": segment,
            "customer.subscription_tier": tier,
            "activation.type": rng.choice(["new_sim", "sim_swap", "plan_upgrade", "5g_activation", "esim_activation"]),
        }
        svc_attrs = {
            "api-gateway": {
                "gateway.protocol": rng.choice(["HTTP/2", "HTTP/1.1", "gRPC"]),
                "gateway.api_version": rng.choice(["v1.4.0", "v1.5.0-beta", "v1.3.2"]),
                "gateway.client_type": rng.choice(["mobile_app", "web_portal", "partner_api", "internal"]),
            },
            "activation-service": {
                "activation.workflow_id": f"WF-{rng.randint(100000, 999999)}",
                "activation.step_count": rng.randint(4, 8),
                "activation.retry_count": rng.randint(0, 3),
            },
            "subscriber-db": {
                "db.query_type": rng.choice(["SELECT", "UPDATE", "INSERT"]),
                "db.table": rng.choice(["subscribers", "activation_log", "subscriber_profiles"]),
                "db.pool_utilization_pct": rng.randint(30, 95),
            },
            "provisioning-service": {
                "provisioning.sim_type": rng.choice(["physical_sim", "esim", "embedded_sim"]),
                "provisioning.hlr_operation": rng.choice(["register", "update", "deregister", "query"]),
                "provisioning.apn": rng.choice(["internet.meridian.eu", "enterprise.meridian.eu", "iot.meridian.eu"]),
            },
            "network-core": {
                "network.bearer_type": rng.choice(["5G_NR", "LTE", "LTE-A", "NR-DC"]),
                "network.qos_class": rng.choice(["QCI-1", "QCI-5", "QCI-9", "5QI-1", "5QI-9"]),
                "network.cell_sector": rng.randint(1, 3),
            },
            "notification-service": {
                "notification.channel": rng.choice(["sms", "push", "email", "webhook"]),
                "notification.template": rng.choice(["activation_confirm", "plan_change", "usage_alert", "partner_callback"]),
                "notification.priority": rng.choice(["high", "normal", "low"]),
            },
            "auth-service": {
                "auth.method": rng.choice(["oauth2", "saml", "api_key", "mtls"]),
                "auth.provider": rng.choice(["internal", "azure_ad", "mvno_partner"]),
                "auth.scope": rng.choice(["activation", "provisioning", "billing", "admin"]),
            },
            "bss-billing": {
                "billing.operation": rng.choice(["account_create", "cdr_rate", "invoice_generate", "credit_apply"]),
                "billing.plan_code": rng.choice(["5G-BASIC-EU-v3", "5G-PLUS-EU-v3", "5G-MAX-EU-v3", "ENT-BUS-L-v2"]),
                "billing.currency": "EUR",
            },
            "identity-db": {
                "db.query_type": rng.choice(["SELECT", "INSERT"]),
                "db.table": rng.choice(["identities", "sessions", "tokens"]),
                "db.replication_role": rng.choice(["primary", "replica"]),
            },
        }
        base.update(svc_attrs.get(service_name, {}))
        return base

    def get_rca_clues(self, channel: int, service_name: str, rng) -> dict:
        clues = {
            1: {  # DB Connection Pool Exhaustion
                "subscriber-db": {"db.pool_active": rng.randint(95, 100), "db.pool_max": 100, "db.wait_queue": rng.randint(200, 500)},
                "provisioning-service": {"upstream.db_latency_ms": rng.randint(3000, 5000), "upstream.timeout_count": rng.randint(30, 80)},
                "activation-service": {"upstream.degraded_subsystem": "database", "activation.failure_rate_pct": round(rng.uniform(15, 55), 1)},
                "api-gateway": {"upstream.degraded_subsystem": "activation", "gateway.5xx_rate": round(rng.uniform(0.1, 0.5), 2)},
            },
            2: {  # RDS Automated Maintenance
                "subscriber-db": {"db.maintenance_active": True, "db.vacuum_table": "subscribers", "db.latency_multiplier": rng.randint(100, 210)},
                "provisioning-service": {"upstream.db_latency_ms": rng.randint(3000, 4500), "upstream.connection_timeout": True},
                "activation-service": {"activation.503_count": rng.randint(200, 1000), "activation.retry_storm": True},
                "bss-billing": {"billing.subscriber_lookup_blocked": True, "billing.pending_activations": rng.randint(50, 300)},
            },
            3: {  # Identity DB Replication Lag
                "identity-db": {"db.replication_lag_ms": rng.randint(5000, 30000), "db.wal_replay_deficit_pct": round(rng.uniform(20, 50), 0)},
                "auth-service": {"auth.stale_session_reads_pct": round(rng.uniform(20, 40), 1), "auth.invalid_token_false_positive": True},
                "api-gateway": {"gateway.auth_failure_rate": round(rng.uniform(0.1, 0.3), 2), "gateway.reauth_storm_risk": "HIGH"},
            },
            4: {  # 5G Bearer Allocation Timeout
                "network-core": {"network.prb_utilization_pct": rng.randint(92, 99), "network.active_bearers": rng.randint(240, 256)},
                "provisioning-service": {"provisioning.bearer_retry_count": rng.randint(2, 3), "provisioning.allocation_queue": rng.randint(50, 200)},
                "activation-service": {"activation.network_timeout_count": rng.randint(20, 100), "activation.degraded_subsystem": "network"},
                "api-gateway": {"gateway.activation_latency_p99_ms": rng.randint(15000, 45000), "gateway.timeout_rate": round(rng.uniform(0.05, 0.2), 2)},
            },
            5: {  # S1-MME Interface Congestion
                "network-core": {"network.sctp_load_pct": rng.randint(85, 98), "network.nas_queue_depth": rng.randint(1000, 5000)},
                "provisioning-service": {"provisioning.attach_delay_ms": rng.randint(500, 3000), "provisioning.signaling_backpressure": True},
                "notification-service": {"notification.paging_storm_detected": True, "notification.throttle_active": True},
            },
            6: {  # GTP Tunnel Setup Failure
                "network-core": {"network.teid_pool_util_pct": rng.randint(90, 99), "network.pgw_status": "RESOURCE_UNAVAILABLE"},
                "activation-service": {"activation.data_path_setup_failed": True, "activation.bearer_ok_tunnel_fail": True},
                "bss-billing": {"billing.activation_incomplete": True, "billing.data_usage_tracking": "BLOCKED"},
            },
            7: {  # OAuth Token Validation Failure
                "auth-service": {"auth.jwks_cache_age_s": rng.randint(3000, 7200), "auth.key_rotation_detected": True},
                "identity-db": {"db.token_lookup_failures": rng.randint(100, 500), "db.cache_miss_rate": round(rng.uniform(0.8, 1.0), 2)},
                "api-gateway": {"gateway.401_rate": round(rng.uniform(0.5, 1.0), 2), "gateway.auth_bypass": False},
                "activation-service": {"activation.auth_blocked": True, "activation.queue_depth": rng.randint(500, 2000)},
            },
            8: {  # AD Federation Timeout
                "auth-service": {"auth.federation_timeout_ms": rng.randint(25000, 35000), "auth.azure_ad_status": "DEGRADED"},
                "api-gateway": {"gateway.enterprise_login_blocked": True, "gateway.consumer_impact": "NONE"},
            },
            9: {  # Session Store Redis Saturation
                "auth-service": {"auth.redis_memory_pct": rng.randint(95, 100), "auth.eviction_rate_per_min": rng.randint(500, 2000)},
                "identity-db": {"db.reauth_connection_spike": rng.randint(200, 500), "db.connection_pool_stress": True},
                "api-gateway": {"gateway.session_failure_rate": round(rng.uniform(0.1, 0.4), 2), "gateway.thundering_herd": True},
                "activation-service": {"activation.auth_dependency_blocked": True, "activation.queue_building": True},
            },
            10: {  # SIM Profile Provisioning Stall
                "provisioning-service": {"provisioning.hlr_queue_depth": rng.randint(3000, 8000), "provisioning.hlr_processing_rate": rng.randint(20, 80)},
                "subscriber-db": {"db.activation_log_insert_queue": rng.randint(500, 2000), "db.pending_status_updates": rng.randint(200, 1000)},
                "activation-service": {"activation.provisioning_timeout": True, "activation.sim_activation_blocked": True},
                "notification-service": {"notification.activation_confirm_delayed": True, "notification.queue_depth": rng.randint(500, 2000)},
            },
            11: {  # Number Portability Lookup Timeout
                "provisioning-service": {"provisioning.crdb_timeout_ms": rng.randint(5000, 15000), "provisioning.mnp_cache_hit": rng.choice([True, False])},
                "activation-service": {"activation.ported_number_blocked": True, "activation.new_msisdn_ok": True},
            },
            12: {  # Tariff Plan Mismatch
                "provisioning-service": {"provisioning.plan_code_lookup": "MISMATCH", "provisioning.affected_tiers": "5G Max, eSIM Premium"},
                "bss-billing": {"billing.plan_code_not_found": True, "billing.orphaned_activations": rng.randint(10, 100)},
                "activation-service": {"activation.billing_rejection_rate": round(rng.uniform(0.05, 0.3), 2), "activation.manual_reconciliation_needed": True},
            },
            13: {  # CDR Processing Backlog
                "bss-billing": {"billing.cdr_queue_depth": rng.randint(50000, 200000), "billing.rating_engine_cpu_pct": rng.randint(90, 99)},
                "subscriber-db": {"db.balance_update_queue": rng.randint(5000, 20000), "db.stale_usage_data": True},
                "notification-service": {"notification.usage_alert_delayed": True, "notification.enterprise_reporting_blocked": True},
            },
            14: {  # Billing Mediation Failure
                "bss-billing": {"billing.mediation_node_status": "FAILED", "billing.duplicate_cdr_count": rng.randint(100, 1000)},
                "activation-service": {"activation.billing_validation_error": True, "activation.new_activations_delayed": True},
                "provisioning-service": {"provisioning.billing_check_failed": True, "provisioning.queue_depth": rng.randint(100, 500)},
            },
            15: {  # Enterprise SLA Breach
                "bss-billing": {"billing.sla_success_rate_pct": round(rng.uniform(92, 98), 1), "billing.breach_duration_min": rng.randint(5, 60)},
                "notification-service": {"notification.sla_alerts_sent": rng.randint(1, 10), "notification.escalation_level": rng.choice(["P1", "P2"])},
                "api-gateway": {"gateway.enterprise_error_rate": round(rng.uniform(0.03, 0.15), 2), "gateway.mvno_error_rate": round(rng.uniform(0.02, 0.1), 2)},
            },
            16: {  # API Rate Limit Cascade
                "api-gateway": {"gateway.rate_limit_exhausted": True, "gateway.retry_amplification": round(rng.uniform(2.0, 5.0), 1)},
                "activation-service": {"activation.429_responses": rng.randint(500, 5000), "activation.client_retry_storm": True},
                "auth-service": {"auth.rate_limited_requests": rng.randint(200, 2000), "auth.legitimate_blocked_pct": round(rng.uniform(30, 70), 0)},
            },
            17: {  # mTLS Certificate Expiry
                "api-gateway": {"gateway.cert_hours_remaining": round(rng.uniform(0.5, 24), 1), "gateway.tls_handshake_failures": rng.randint(100, 5000)},
                "auth-service": {"auth.mtls_validation_status": "WARNING", "auth.cert_chain_valid": False},
                "activation-service": {"activation.upstream_tls_errors": rng.randint(50, 500), "activation.service_degraded": True},
            },
            18: {  # SMS Gateway Delivery Failure
                "notification-service": {"notification.smpp_bind_state": "UNBOUND", "notification.sms_queue_depth": rng.randint(500, 5000)},
                "activation-service": {"activation.confirm_sms_pending": rng.randint(100, 1000), "activation.notification_timeout": True},
            },
            19: {  # Push Notification Backpressure
                "notification-service": {"notification.push_rate_limited": True, "notification.fcm_429_count": rng.randint(200, 2000)},
                "activation-service": {"activation.notification_ack_delayed": True, "activation.completion_blocked": True},
                "provisioning-service": {"provisioning.notification_dependency_timeout": True, "provisioning.async_queue_building": True},
            },
            20: {  # MVNO Webhook Timeout
                "notification-service": {"notification.webhook_circuit_breaker": "OPEN", "notification.failed_callbacks": rng.randint(50, 500)},
                "api-gateway": {"gateway.mvno_status_queries_spike": rng.randint(500, 3000), "gateway.partner_api_errors": True},
                "bss-billing": {"billing.unacknowledged_activations": rng.randint(20, 200), "billing.wholesale_reconciliation": "BLOCKED"},
            },
        }
        channel_clues = clues.get(channel, {})
        return channel_clues.get(service_name, {})

    def get_correlation_attribute(self, channel: int, is_error: bool, rng) -> dict:
        correlation_attrs = {
            1: ("deployment.hikari_pool_config", "hikari-v5.1.0-custom-patch"),
            2: ("infra.rds_param_group", "pg15-custom-v2.3.1"),
            3: ("infra.azure_pg_replica_sku", "flex-replica-v1.2.0-rc1"),
            4: ("network.gnb_firmware", "gnb-v4.8.0-rc2"),
            5: ("network.mme_firmware", "mme-v8.2.0-rc1"),
            6: ("network.pgw_config_version", "pgw-5g-v3.1.0-patch"),
            7: ("auth.jwks_cache_version", "jwks-v3.1.0-stale"),
            8: ("auth.saml_library_version", "saml2-v2.4.1-hotfix"),
            9: ("infra.redis_cluster_config", "redis-7.2.4-sentinel-patch"),
            10: ("provisioning.hlr_connector_version", "hlr-conn-v6.0.1-beta"),
            11: ("provisioning.crdb_client_version", "crdb-client-v1.8.0-rc3"),
            12: ("deployment.tariff_mapping_version", "tariff-map-v2026.03.05"),
            13: ("billing.rating_engine_build", "rating-v4.2.1-retrain"),
            14: ("billing.mediation_node_config", "mediation-v3.0.0-rc2"),
            15: ("billing.sla_monitor_config", "sla-monitor-v2.1.0-patch"),
            16: ("deployment.rate_limiter_config", "ratelimit-v1.5.0-aggressive"),
            17: ("deployment.cert_manager_version", "cert-manager-v1.14.5-rc1"),
            18: ("notification.smpp_gateway_version", "smpp-gw-v4.2.0-hotfix"),
            19: ("notification.push_sdk_version", "fcm-sdk-v24.1.0-beta"),
            20: ("notification.webhook_client_version", "webhook-v2.3.0-timeout-patch"),
        }
        attr_key, attr_val = correlation_attrs.get(channel, ("deployment.unknown", "unknown"))
        # 90% on errors, 5% on healthy
        if is_error:
            if rng.random() < 0.90:
                return {attr_key: attr_val}
        else:
            if rng.random() < 0.05:
                return {attr_key: attr_val}
        return {}

    # ── Fault Parameters ──────────────────────────────────────────────

    def get_fault_params(self, channel: int) -> dict[str, Any]:
        msisdns = [
            "+49151" + str(random.randint(10000000, 99999999)),
            "+49160" + str(random.randint(10000000, 99999999)),
            "+49170" + str(random.randint(10000000, 99999999)),
        ]
        iccids = [f"8949{random.randint(1000000000000000, 9999999999999999)}" for _ in range(3)]
        cell_ids = [f"CELL-EU-{random.randint(10000, 99999)}" for _ in range(3)]
        gnodeb_ids = [f"gNB-FRA-{random.randint(100, 999)}" for _ in range(3)]
        mme_ids = [f"MME-EU-{random.randint(1, 8):02d}" for _ in range(3)]
        sgw_ids = [f"SGW-FRA-{random.randint(1, 4):02d}" for _ in range(3)]
        pgw_ids = [f"PGW-FRA-{random.randint(1, 4):02d}" for _ in range(3)]
        mvno_partners = ["Lycamobile", "Lebara", "Aldi Talk", "Congstar"]
        donor_operators = ["Vodafone DE", "Deutsche Telekom", "1&1", "Drillisch"]
        sms_aggregators = ["Nexmo", "Twilio", "MessageBird", "Infobip"]
        federation_providers = ["Azure AD", "Okta", "OneLogin"]
        federation_tenants = ["meridian-enterprise", "partner-sso", "mvno-auth"]
        plan_codes = ["5G-BASIC-EU-v3", "5G-PLUS-EU-v3", "5G-MAX-EU-v3", "ESIM-PREM-v2"]
        bss_codes = ["PROD-5G-2024-BASIC", "PROD-5G-2024-PLUS", "PROD-5G-2024-MAX", "PROD-ESIM-2024"]
        tariff_errors = ["PLAN_CODE_NOT_FOUND", "MAPPING_VERSION_MISMATCH", "BSS_PRODUCT_DEPRECATED"]
        sla_segments = ["Enterprise", "MVNO"]
        sla_credit_tiers = ["WARNING", "5_PCT_CREDIT", "10_PCT_CREDIT", "20_PCT_CREDIT"]
        cert_domains = ["api.meridian-telecom.eu", "auth.meridian-telecom.eu", "portal.meridian-telecom.eu"]
        cert_issuers = ["DigiCert", "AWS_ACM", "Let's_Encrypt"]
        webhook_endpoints = [
            "https://api.lycamobile.eu/v1/activation/callback",
            "https://api.lebara.eu/v2/activation/webhook",
            "https://partners.alditalk.de/api/activation/notify",
        ]
        smpp_statuses = ["UNBOUND", "BIND_FAILED", "CONNECTION_LOST", "THROTTLED"]
        sms_errors = ["SMPP_BIND_FAILED", "CONNECTION_TIMEOUT", "ESME_RTHROTTLED", "DELIVERY_FAILED"]
        push_platforms = ["FCM", "APNs", "HMS"]
        bearer_types = ["5G_NR", "LTE", "NR-DC", "LTE-A"]
        token_issuers = ["https://idp.meridian.eu", "https://login.microsoftonline.com/meridian", "https://auth.mvno-partner.eu"]
        token_errors = ["INVALID_SIGNATURE", "KEY_NOT_FOUND", "TOKEN_EXPIRED", "ISSUER_MISMATCH"]
        gtp_causes = ["RESOURCE_UNAVAILABLE", "TEID_POOL_EXHAUSTED", "PGW_OVERLOADED", "APN_MISMATCH"]
        mediation_nodes = ["mediation-01", "mediation-02", "mediation-03"]
        mediation_partitions = ["P0,P1", "P2,P3", "P4,P5"]
        sim_types = ["physical_sim", "esim", "embedded_sim"]
        sim_stages = ["hlr_registration", "profile_download", "ota_activation"]
        port_types = ["port_in", "port_out", "internal_transfer"]
        subscription_tiers = ["5G Basic", "5G Plus", "5G Max", "Business S", "Business M", "Business L"]
        gw_sources = ["mobile_app_retry", "partner_api_retry", "internal_retry", "load_test_leak"]

        return {
            # Subscriber identifiers
            "msisdn": random.choice(msisdns),
            "sim_iccid": random.choice(iccids),
            "customer_id": f"CUST-{random.randint(100000, 999999)}",
            # DB pool
            "pool_active": random.randint(95, 100),
            "pool_max": 100,
            "wait_queue": random.randint(200, 500),
            "avg_wait_ms": random.randint(3000, 8000),
            "db_host": "subscriber-db.eu-central-1.rds.amazonaws.com",
            "error_rate_pct": round(random.uniform(10, 55), 1),
            # RDS maintenance
            "db_latency_ms": random.randint(2000, 4500),
            "db_baseline_ms": random.randint(15, 25),
            "rds_event": "ModifyDBInstance",
            "maintenance_duration_min": random.randint(5, 25),
            "event_time": "2026-03-08T07:12:00Z",
            "latency_multiplier": random.randint(100, 210),
            "failed_activations": random.randint(500, 5000),
            "affected_subscribers": random.randint(5000, 15000),
            # Replication
            "repl_lag_ms": random.randint(5000, 30000),
            "repl_max_ms": 3000,
            "repl_node": "identity-db-replica",
            "repl_pending_txns": random.randint(5000, 50000),
            "stale_reads_pct": round(random.uniform(20, 40), 1),
            # Bearer allocation
            "cell_id": random.choice(cell_ids),
            "gnodeb_id": random.choice(gnodeb_ids),
            "bearer_type": random.choice(bearer_types),
            "bearer_timeout_ms": random.randint(5000, 15000),
            "prb_util_pct": random.randint(92, 99),
            "active_bearers": random.randint(240, 256),
            "queued_activations": random.randint(20, 100),
            # S1-MME
            "mme_id": random.choice(mme_ids),
            "sctp_load_pct": random.randint(85, 98),
            "nas_queue_depth": random.randint(1000, 5000),
            "attach_delay_ms": random.randint(500, 3000),
            "mme_region": "EU-CENTRAL",
            "attach_multiplier": random.randint(5, 30),
            # GTP tunnel
            "tunnel_id": f"TUN-{random.randint(100000, 999999)}",
            "sgw_id": random.choice(sgw_ids),
            "pgw_id": random.choice(pgw_ids),
            "gtp_cause": random.choice(gtp_causes),
            "teid_util_pct": random.randint(90, 99),
            "teid_sgw": f"{random.randint(0x10000000, 0xFFFFFFFF):08X}",
            # Auth token
            "token_issuer": random.choice(token_issuers),
            "token_error": random.choice(token_errors),
            "jwks_cache_age_s": random.randint(3000, 7200),
            "token_kid": f"kid-{random.randint(1000, 9999)}",
            "auth_affected_requests": random.randint(500, 5000),
            # Federation
            "federation_provider": random.choice(federation_providers),
            "federation_timeout_ms": random.randint(25000, 35000),
            "federation_tenant": random.choice(federation_tenants),
            "sso_blocked_count": random.randint(100, 1000),
            # Redis session
            "redis_memory_pct": random.randint(95, 100),
            "redis_session_count": random.randint(500000, 1000000),
            "redis_evictions": random.randint(500, 2000),
            "reauth_rate": random.randint(1000, 5000),
            # SIM provisioning
            "hlr_queue_depth": random.randint(3000, 8000),
            "hlr_processing_rate": random.randint(20, 80),
            "hlr_stall_ms": random.randint(10000, 60000),
            "hlr_deficit": random.randint(100, 180),
            "sim_type": random.choice(sim_types),
            "sim_stage": random.choice(sim_stages),
            # Number portability
            "donor_operator": random.choice(donor_operators),
            "crdb_timeout_ms": random.randint(5000, 15000),
            "mnp_cache_hit": random.choice(["true", "false"]),
            "port_type": random.choice(port_types),
            # Tariff
            "plan_code": random.choice(plan_codes),
            "bss_code": random.choice(bss_codes),
            "tariff_error": random.choice(tariff_errors),
            "subscription_tier": random.choice(subscription_tiers),
            "orphaned_count": random.randint(10, 100),
            # CDR
            "cdr_queue_depth": random.randint(50000, 200000),
            "cdr_ingestion_rate": random.randint(2000, 5000),
            "cdr_processing_rate": random.randint(500, 1500),
            "cdr_oldest_min": random.randint(15, 120),
            "cdr_backlog_gb": round(random.uniform(2.0, 20.0), 1),
            "cdr_deficit": random.randint(500, 3500),
            # Mediation
            "mediation_node": random.choice(mediation_nodes),
            "mediation_partition": random.choice(mediation_partitions),
            "duplicate_cdr_count": random.randint(100, 1000),
            "checkpoint_age_min": random.randint(5, 60),
            "cdrs_at_risk": random.randint(5000, 50000),
            # SLA breach
            "sla_segment": random.choice(sla_segments),
            "sla_success_rate": round(random.uniform(92, 98.5), 1),
            "sla_target": 99.5,
            "sla_breach_min": random.randint(5, 60),
            "sla_credit_tier": random.choice(sla_credit_tiers),
            "sla_activation_time_s": random.randint(35, 120),
            "breach_start_time": "2026-03-08T07:12:00Z",
            "sla_credit_amount": round(random.uniform(5000, 50000), 0),
            "enterprise_affected": random.randint(5, 50),
            "mvno_affected": random.randint(1, 5),
            "notifications_sent": random.randint(3, 15),
            # Rate limiter
            "gw_current_rate": random.randint(2000, 8000),
            "gw_rate_limit": 1000,
            "gw_rejected_count": random.randint(500, 5000),
            "gw_burst_factor": round(random.uniform(2.0, 5.0), 1),
            "gw_source": random.choice(gw_sources),
            "gw_unique_clients": random.randint(50, 500),
            # Certificate
            "cert_domain": random.choice(cert_domains),
            "cert_hours_left": round(random.uniform(0.5, 48.0), 1),
            "cert_serial": f"{random.randint(10000000, 99999999):08X}",
            "cert_issuer": random.choice(cert_issuers),
            "cert_svc_count": random.randint(2, 5),
            # SMS
            "smpp_status": random.choice(smpp_statuses),
            "sms_delivery_rate": round(random.uniform(0, 60), 1),
            "sms_queued": random.randint(500, 5000),
            "sms_aggregator": random.choice(sms_aggregators),
            "sms_error": random.choice(sms_errors),
            "sms_failed": random.randint(200, 2000),
            "activation_confirms_pending": random.randint(100, 1000),
            # Push
            "push_platform": random.choice(push_platforms),
            "push_current_rate": random.randint(500, 2000),
            "push_rate_limit": 500,
            "push_queue_depth": random.randint(1000, 10000),
            "push_backoff_ms": random.randint(1000, 8000),
            "activation_confirms_delayed": random.randint(200, 2000),
            "priority_high_queued": random.randint(100, 500),
            "priority_low_queued": random.randint(500, 5000),
            # MVNO webhook
            "mvno_partner": random.choice(mvno_partners),
            "webhook_endpoint": random.choice(webhook_endpoints),
            "webhook_timeout_ms": random.randint(5000, 30000),
            "webhook_failures": random.randint(50, 500),
            "webhook_pending": random.randint(20, 200),
            "webhook_success_rate": round(random.uniform(10, 60), 1),
        }


# Module-level instance for registry discovery
scenario = TelecomScenario()
