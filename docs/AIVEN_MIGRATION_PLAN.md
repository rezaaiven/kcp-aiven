# Plan: Confluent Cloud → Aiven for Kafka Migration

This document outlines how to extend KCP to support **migrating from Confluent Cloud to Aiven for Kafka**, as an alternative to the current flow (AWS MSK → Confluent Cloud).

---

## Current vs desired flow

| | Current (KCP) | Desired (Aiven migration) |
|---|---------------|----------------------------|
| **Source** | AWS MSK (discover via AWS APIs) | Confluent Cloud (discover via Confluent APIs) |
| **Target** | Confluent Cloud (Terraform + cluster links) | Aiven for Kafka (Terraform + linking/replication) |
| **State** | `kcp-state.json` (regions = AWS, clusters = MSK) | State must represent Confluent clusters or be source-agnostic |
| **Reports** | AWS Cost Explorer + CloudWatch | Confluent usage/billing (optional) or topology-only |
| **Assets** | Confluent Terraform, MSK→CCloud links | Aiven Terraform, CCloud→Aiven linking (e.g. MirrorMaker 2) |

---

## High-level approach

Introduce **source type** and **target type** so the same CLI can support:

1. **MSK → Confluent Cloud** (existing)
2. **Confluent Cloud → Aiven** (new)

Prefer **additive changes** and **new code paths** over rewriting existing MSK/Confluent logic, so current behavior stays intact.

---

## Phase 1: Confluent Cloud discovery (source)

**Goal:** Populate a state file by discovering Confluent Cloud resources instead of AWS MSK.

### 1.1 Confluent Cloud API client

- **Location:** `internal/client/confluent_cloud.go` (new).
- **Dependencies:** Confluent Cloud API (REST). Auth: API key + secret (or service account).
- **Implement:**
  - List environments (or use a single environment ID from config).
  - List Kafka clusters in environment (`GET /kafka/v3/clusters` or equivalent).
  - For each cluster: get bootstrap endpoints, cluster ID, REST endpoint.
  - Optional: list topics via Kafka REST API v3 or AdminClient; list ACLs if API exists.
- **References:** [Confluent Cloud API](https://docs.confluent.io/cloud/current/api.html), [Kafka REST API v3](https://docs.confluent.io/cloud/current/kafka-rest/kafka-rest-cc.html).

### 1.2 State model for Confluent source

Two options:

- **Option A – New state file shape:** e.g. `kcp-state-confluent.json` with `ConfluentState` containing environments, clusters, topics (no AWS regions). Downstream (report, create-asset) branch on “which state type do I have?”
- **Option B – Extend existing State:** Add optional `SourceType string` (`"aws_msk"` \| `"confluent_cloud"`) and optional `ConfluentClusters []ConfluentClusterInfo` (environment id, cluster id, bootstrap, rest endpoint, topics, etc.). Keep `Regions` for MSK; when `SourceType == "confluent_cloud"`, use `ConfluentClusters` instead.

Recommendation: **Option B** so report and create-asset can share one state structure and branch on `SourceType`.

### 1.3 Discover command for Confluent

- **Option A:** New command `kcp discover-confluent` that uses Confluent client and writes state (with `SourceType = "confluent_cloud"` and `ConfluentClusters` populated).
- **Option B:** `kcp discover --source confluent` (and keep `--source aws` or default = AWS). Single discover entrypoint; backend chosen by flag.

Implement:

- Confluent Cloud API client (above).
- A “Confluent region discoverer” (no AWS regions; “region” might be environment or cloud region in Confluent).
- Fill `State.SourceType` and `State.ConfluentClusters` (or equivalent).
- Reuse or stub cost/metrics (e.g. leave empty or add Confluent usage API later).

### 1.4 Scan (optional for Phase 1)

- `kcp scan clusters` today reads MSK state and credentials. For Confluent source, you’d need `kcp scan confluent-clusters` (or `scan clusters --state-file <confluent-state>`) that uses Confluent REST/Admin to list topics, ACLs, etc., and merge into state. Can be Phase 2.

---

## Phase 1 re-review (pre-implementation)

This section re-reviews Phase 1 against the codebase and the critical review (Section “Critical review: weaknesses, risks, and alternatives”) so implementation can proceed with clear decisions.

### Decisions to lock in before implementation

| Topic | Original Phase 1 | Re-review recommendation | Rationale |
|-------|------------------|--------------------------|-----------|
| **State model** | Option B: extend `State` with `SourceType` + `ConfluentClusters` | **Use a separate Confluent state shape** (Section 3.1), not Option B | Every consumer of `State` assumes AWS/MSK: `state.Regions`, `GetClusterByArn(clusterArn)`, `WriteReportCommands`, report costs/metrics (`len(state.Regions) == 0` → error). Extending `State` would require branches or fallbacks in report, UI API, frontend, and create-asset. Empty `Regions` for Confluent breaks existing report commands. |
| **Discover command** | Option A or B: new command vs `discover --source confluent` | **Either** `kcp discover-confluent` **or** `kcp discover --source confluent` that **delegates to a separate implementation** writing **only** Confluent state | No change to existing `State` or `NewStateFrom` for MSK. Same binary can support both; implementation writes Confluent-specific state file (e.g. same filename with discriminator or dedicated `kcp-state-confluent.json`). |
| **Credentials** | Not specified | **Define in Phase 1:** where Confluent API key/secret are stored and how discover (and later scan/create-asset) look them up | Current credentials are `cluster-credentials.yaml` with `Regions` and MSK-specific auth. Confluent uses API key + secret (org/cluster scope). Options: new file (e.g. `confluent-credentials.yaml`), new top-level key in existing file, or env vars. Must be decided so the Confluent client and discover command can be implemented. |
| **Confluent client** | List envs, clusters, bootstrap, REST; optional topics/ACLs | **Add:** retries + backoff for rate limits (429); **document** which API key type (org vs cluster) is required for Cloud API vs Kafka REST | Critical review 1.4: Confluent APIs may throttle; auth scope (cluster vs org) must be clear for listing environments/clusters and for Kafka REST. |
| **Testing** | Not in Phase 1 | **Include in Phase 1:** tests for Confluent discovery using **mocked Confluent API responses** (no live Confluent account required in CI) | Critical review 1.6: avoids regressions and API compatibility issues; live-API/contract tests can be added later. |

### Phase 1 scope (revised)

1. **Confluent Cloud API client** (`internal/client/confluent_cloud.go`)
   - List environments (or single environment from config), list Kafka clusters, bootstrap + REST endpoint per cluster.
   - Optional: topics via Kafka REST v3 / AdminClient; ACLs if API exists.
   - **Must have:** retries and backoff for rate limits; documented auth scope (API key type and usage).

2. **Confluent state model (separate shape)**
   - New type, e.g. `ConfluentMigrationState` (or state file with discriminator `"source_type": "confluent_cloud"` and structure like `environments` / `confluent_clusters`), **no** `Regions` or Option B extension of current `State`.
   - Persist to same file name with discriminator or to `kcp-state-confluent.json`; loader can return either MSK `State` or Confluent state for downstream commands.

3. **Confluent credentials**
   - Design: where Confluent API key/secret are stored and how they are looked up (file path, env vars, or key in existing credentials file).
   - Implement the lookup so the Confluent client and discover command can run without hardcoded credentials.
   - **Implemented:** `confluent-credentials.yaml` (or env: `CONFLUENT_API_KEY`, `CONFLUENT_API_SECRET`, `CONFLUENT_ENVIRONMENT_ID`); see `internal/types/confluent_credentials.go` and `docs/confluent-credentials.example.yaml`.

4. **Discover for Confluent**
   - New command or `discover --source confluent` that: uses Confluent client and credentials, runs “Confluent discoverer” (no AWS regions), writes **only** Confluent state (no change to existing `State` or MSK discover path).

5. **Tests**
   - Unit tests for Confluent client and discover using **mocked** Confluent API responses (and optionally a small test that validates state file shape).

6. **Live API tests (optional)**
   - Tests that run against the **real** Confluent Cloud API when credentials are available. Built only with `-tags=confluent_live` so they do not run in normal CI. Use for integration/contract verification.
   - **Run:** Set `CONFLUENT_API_KEY` and `CONFLUENT_API_SECRET` (or `confluent-credentials.yaml`), then: `go test -tags=confluent_live ./cmd/discover/... ./internal/client/...`
   - **Files:** `cmd/discover/confluent_discoverer_live_test.go`, `internal/client/confluent_cloud_live_test.go`

### Out of scope for Phase 1 (explicit)

- **Report costs/metrics** for Confluent state: not in Phase 1; later a separate “report topology” (or `report confluent-topology`) can read Confluent state and output topology only (no AWS cost/metrics).
- **Scan** of Confluent clusters (topics, ACLs): remains optional / Phase 2.
- **UI, create-asset, Aiven:** all later phases.

### Implementation order for Phase 1

1. Confluent credentials: **design and implement** storage + lookup.
2. Confluent Cloud API client with retries, backoff, and documented auth scope.
3. Confluent state type and file format (discriminator or separate file).
4. Discover command (confluent) that writes Confluent state only.
5. Tests: mocked Confluent API for client and discover.
6. Live API tests: optional tests against real Confluent Cloud API (`go test -tags=confluent_live ...`).

This keeps the existing MSK → Confluent flow untouched and makes the Confluent → Aiven path a separate, first-class pipeline from discovery onward.

---

## Phase 2: Aiven as target (create-asset)

**Goal:** Generate Terraform and migration assets that create Aiven for Kafka and link/mirror **from** Confluent Cloud **to** Aiven.

### 2.1 Aiven Terraform (target infra)

- **Location:** `internal/services/hcl/aiven/` (new), plus a new module or branch in `internal/services/hcl/target_infra_hcl_service.go`.
- **Resources (examples):**
  - `aiven_kafka` (or equivalent in [Aiven Terraform provider](https://registry.terraform.io/providers/aiven/aiven/latest/docs/resources/kafka)) for the target Kafka service.
  - `aiven_kafka_topic` if you want topic creation on Aiven side.
  - [Aiven: Integrate external Kafka cluster](https://aiven.io/docs/products/kafka/howto/integrate-external-kafka-cluster): external cluster config (bootstrap, security) so Aiven can connect to Confluent Cloud.
- **CLI:** Either:
  - New command: `kcp create-asset target-infra-aiven` (output dir, project name, cloud/region, Confluent bootstrap as “source”), or
  - Extend `create-asset target-infra` with `--target aiven` and a new request type (e.g. `AivenTargetRequest`) that drives Aiven HCL generation instead of Confluent.
- **Implemented (Phase 2.1):** `kcp create-asset target-infra-aiven` with `--project`, `--cloud-name`, `--plan`, `--service-name`, `--output-dir`, `--prevent-destroy`. Aiven credentials via `AIVEN_TOKEN` / `AIVEN_API_TOKEN` or `aiven-credentials.yaml` (see `docs/aiven-credentials.example.yaml`). Generated Terraform uses Aiven provider and `aiven_kafka`; Kafka SASL credentials are obtained from Aiven after the service is created.

- **Possible follow-ups (Phase 2.1, not done):**
  - **Optional credentials check in PreRunE:** Call `GetAivenCredentials(DefaultAivenCredentialsFileName)` in the CLI and fail fast if neither env nor file is set. Deferred because it would break “generate-only” workflows (e.g. CI that only writes Terraform and runs apply elsewhere with AIVEN_TOKEN).
  - **Stricter `cloud_name` format:** Validate pattern (e.g. `provider-region` like `aws-eu-west-1`). Aiven’s exact rules are not clearly documented; Terraform will fail on apply if invalid. Can be added later if we get official constraints.
  - **Warn when output dir exists:** Emit a warning if `--output-dir` already exists and has content, to avoid overwriting. Current behavior matches existing `target-infra`; can be added for consistency.

### 2.2 Cluster linking / replication (Confluent → Aiven)

- **Confluent side:** Cluster link **source** = Confluent cluster; **destination** = Aiven (bootstrap). So link is created on Confluent pointing to Aiven, or mirror topics on Confluent from… no – we want data to move **to** Aiven.
- **Aiven side:** Aiven supports [external Kafka integration](https://aiven.io/docs/products/kafka/howto/integrate-external-kafka-cluster) and [MirrorMaker 2](https://aiven.io/developer/kafka-mirrormaker-crosscluster). So the **link/replication** should be created **on Aiven**, with Confluent Cloud as the external/source cluster.
- **Implementation:**
  - Add Aiven client or Terraform resources for “external Kafka” / MirrorMaker 2 source = Confluent (bootstrap + auth).
  - New create-asset subcommand or flag: e.g. `create-asset migrate-topics --target aiven` that generates Terraform/scripts to create mirror/replication **from** Confluent **to** Aiven (not the current Confluent-centric mirror topic resource).
- **Auth:** Confluent Cloud supports API key, SASL; Aiven supports SASL. Ensure auth params (bootstrap, API key or SASL) are passed through to Aiven’s external cluster / MirrorMaker config.

#### External Kafka integration vs MirrorMaker 2: one path, both required

**They are not alternatives.** Use both, in this order:

| Concept | What it is | Role in Confluent → Aiven |
|--------|------------|----------------------------|
| **External Kafka integration** | Aiven’s way to define a *connection* to an external cluster: bootstrap servers, security protocol (e.g. SASL_SSL), and credentials. | Defines **Confluent Cloud as the source endpoint** (bootstrap, SASL_SSL, API key/secret). No replication by itself. |
| **MirrorMaker 2** | The replication engine (topic sync, offsets, heartbeats). Aiven offers it as a **managed service**. | Does the actual **replication** from source → Aiven Kafka. It *uses* the external Kafka integration to reach Confluent. |

**Recommended implementation path:** **MirrorMaker 2**, with the **external Kafka integration** as the source configuration.

1. Generate Terraform/config for an **external Kafka integration** that points at Confluent Cloud (bootstrap, SASL_SSL, credentials).
2. Generate Terraform/config for an **Aiven MirrorMaker 2** service and **replication flows** where source = that integration (Confluent), target = Aiven Kafka.

So: **MirrorMaker 2 is the replication path**; **external Kafka integration is how we tell MirrorMaker 2 where Confluent Cloud is.** KCP’s create-asset should output both (integration + MM2 service + flows). Do not treat “external integration only” as a migration path—it only defines the connection; replication is done by MirrorMaker 2.

### 2.3 Migrate topics, schemas, ACLs

- **Topics:** Drive mirror/replication setup (above) so topics on Confluent are mirrored to Aiven. Optionally generate `aiven_kafka_topic` for explicit create (e.g. if not using mirror for some topics).
- **Schemas:** If state has schema registry info for Confluent, add step to export/import schemas to Aiven Schema Registry (Aiven has Schema Registry); can be a new `create-asset migrate-schemas --target aiven` or extend existing migrate-schemas.
- **ACLs:** Map Confluent ACLs (or RBAC) to Aiven ACLs if Aiven supports them via API/Terraform; add `create-asset migrate-acls` path for Aiven target.

---

## Phase 3: Reports and UI

- **Reports:** For state with `SourceType == "confluent_cloud"`, skip or replace AWS Cost Explorer/CloudWatch with:
  - Confluent usage/billing API (if available and desired), or
  - Topology-only report (clusters, topics, config) without cost/metrics.
- **UI:** Ensure `kcp ui` can load and display Confluent-sourced state (same state file, different shape for clusters). May require small changes in how cluster list and details are rendered (e.g. show Confluent cluster id, bootstrap, environment).

---

## Phase 4: Cleanup and docs

- **Flags and commands:** Document `discover --source confluent`, `create-asset target-infra --target aiven`, `create-asset migrate-topics --target aiven`, etc.
- **README:** Describe two flows: MSK → Confluent Cloud and Confluent Cloud → Aiven.
- **Backward compatibility:** Default `discover` to AWS MSK; default `create-asset target-infra` to Confluent so existing users unchanged.

---

## Suggested file/package layout (additive)

```
internal/client/
  confluent_cloud.go          # NEW: Confluent Cloud API client

internal/services/
  confluent_discover/         # Optional: Phase 1 instead put discover logic in cmd/discover/confluent_discoverer.go
    discoverer.go
  hcl/
    aiven/                    # NEW: Aiven Terraform resources
      kafka.go
      external_cluster.go     # or mirror maker config
    target_infra_hcl_service.go  # EXTEND: branch on target type (confluent | aiven)

internal/types/
  state.go                    # EXTEND: SourceType, ConfluentClusters (or ConfluentState)
  types.go                    # EXTEND: AivenTargetRequest if needed

cmd/
  discover/
    cmd_discover.go           # existing MSK discover
    cmd_discover_confluent.go # Phase 1: kcp discover-confluent
    confluent_discoverer.go   # Phase 1: Confluent discover logic
  create_asset/
    target_infra/
      cmd_create_asset_target_infra.go  # EXTEND: --target aiven
    migrate_topics/
      ...                     # EXTEND: support --target aiven (Aiven as mirror dest)
```

---

## Implementation order

1. **Phase 1.1 + 1.2:** Confluent Cloud client + state model extension.
2. **Phase 1.3:** `discover` for Confluent (new or flag).
3. **Phase 2.1:** Aiven target infra (Terraform).
4. **Phase 2.2:** Confluent → Aiven linking/replication (MirrorMaker 2 or external cluster).
5. **Phase 2.3:** Topics/schemas/ACLs for Aiven target.
6. **Phase 3:** Reports + UI for Confluent state.
7. **Phase 4:** Docs and defaults.

---

## Confluent Cloud vs Aiven: migration risks and differences

Below are differences that could block migration, require manual steps, or produce a different security/feature profile on Aiven. Addressing these in discovery, generated assets, or documentation reduces the risk of “cannot migrate” or post-migration surprises.

| Area | Confluent Cloud | Aiven for Kafka | Migration risk / mitigation |
|------|-----------------|-----------------|-----------------------------|
| **Auth & access control** | RBAC (roles, role bindings) + ACLs (ACLALLOW/ACLDENY). Principals can have both. | **Aiven governance** includes **RBAC** (limited availability) for fine-grained permissions, plus **Aiven ACLs** and **Kafka-native ACLs**. See [Aiven for Apache Kafka® governance](https://aiven.io/docs/products/kafka/concepts/governance-overview). | **Re-assessed:** Aiven now offers RBAC as part of governance (LA). Risk shifts to **mapping Confluent RBAC roles/semantics to Aiven governance RBAC** (role names, bindings, and scope may differ). **Mitigation:** Discover Confluent RBAC and ACLs; document Confluent→Aiven role/permission mapping; use Aiven governance RBAC where available; flag unsupported or manual mappings. |
| **Schema Registry** | Confluent Schema Registry (proprietary, part of platform). | **Karapace** (open-source), API-compatible with Confluent Schema Registry up to ~6.1.1. Same formats: Avro, Protobuf, JSON. | **Risk:** Low for schema data itself. Revisit if compatibility, auth, or subject-naming issues appear later. **Mitigation:** Use standard Schema Registry export/import or MirrorMaker 2; validate compatibility rules and subject names. |
| **Topic / partition / message limits** | Limits vary by cluster type (Basic, Standard, Dedicated). Partition limits per cluster; `max.message.bytes` up to 10 MB (e.g. 8 MB request size for non-Dedicated). | Configurable (e.g. `kafka.message_max_bytes`). Recommendations: e.g. &lt;4k partitions per broker, &lt;200k per cluster, &lt;7k topics. | **Risk:** Topics or producer/consumer configs that rely on Confluent’s higher limits may hit Aiven limits or defaults. **Mitigation:** During discovery, record topic configs and partition counts; warn when values exceed Aiven recommendations; see “Topic and cluster configs with migration risk” below for specific config names. |
| **Managed connectors** | Confluent fully managed connectors (120+; some cloud-only). | Aiven: Kafka Connect with its own [connector catalog](https://aiven.io/docs/products/kafka/kafka-connect/concepts/list-of-connector-plugins) (Debezium, S3, Snowflake, Couchbase, ClickHouse, etc.). | **Risk:** Some Confluent proprietary/cloud-only connectors may have no direct Aiven equivalent. **Mitigation:** Discover Confluent connector types; map to Aiven equivalents where they exist; see “Confluent proprietary connectors and Aiven alternatives” below. |
| **ksqlDB / streaming SQL** | Confluent Cloud: managed ksqlDB. | No managed ksqlDB; **Aiven for Apache Flink** available. **Aiven for ClickHouse** is often suggested as a functional substitution for certain analytics/streaming use cases (to be discussed separately). | **Risk:** ksqlDB apps cannot be migrated as-is to a managed Aiven service. **Mitigation:** Document options: self-hosted ksqlDB, Flink, or ClickHouse for substitution; defer detailed ClickHouse discussion to later. |
| **Stream Catalog / governance** | Confluent Stream Catalog (metadata, tags, classifications, RBAC). | Aiven Kafka Topic Catalog + **governance** (LA). **Klaw** may be used for topic/ACL governance but may not be available soon. **DataHub** (under heavy development) is a platform-agnostic metadata catalog with Kafka ingestion and can be a possible alternative for catalog/governance. | **Risk:** Metadata and governance do not transfer automatically. **Mitigation:** Re-establish on Aiven (Topic Catalog, governance, or DataHub); document Klaw/DataHub as alternatives where applicable. |
| **Client authentication** | SASL_SSL with PLAIN (API key as username, secret as password). | Multiple options (SASL PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, certs). Credential model differs. | **Risk:** Clients must be reconfigured for Aiven credentials. **Mitigation:** Document Confluent vs Aiven credentials and auth (see “Confluent vs Aiven credentials and authentication” below). |
| **Exactly-once / transactions** | Supported. | Supported (e.g. MirrorMaker 2 exactly-once delivery). | **Risk:** Low if both sides support the same semantics; config (e.g. MirrorMaker 2) must be set correctly. **Mitigation:** Use Aiven’s exactly-once options for replication where required; document any Confluent-specific transactional settings that need an Aiven equivalent. |
| **Private connectivity** | Confluent Private Link, VPC peering (cloud-specific). | Aiven VPC peering, integration with cloud networks. | **Risk:** Network topology is different; not a data-migration blocker but operational. **Mitigation:** Document that private connectivity must be designed for Aiven (e.g. peering, PrivateLink where available); target-infra can generate Aiven networking where applicable. |

**Summary of “hard” migration risks**

- **RBAC:** Aiven has governance RBAC (LA); map Confluent RBAC roles/semantics to Aiven governance where possible; some manual mapping may be needed.
- **Connectors:** Some Confluent proprietary/cloud-only connectors have no direct Aiven equivalent (e.g. Salesforce family, Datagen); use Aiven alternatives or custom solutions where available.
- **ksqlDB:** No managed ksqlDB on Aiven; self-hosted ksqlDB, Flink, or ClickHouse as functional substitution (ClickHouse discussion deferred).
- **Limits:** Specific topic/cluster configs can differ; validate and adjust (see config list below).
- **Stream Catalog / metadata:** Not automatically migrated; re-establish via Aiven Topic Catalog, governance, Klaw (when available), or DataHub.

In the KCP plan, we can **discover** Confluent ACLs and RBAC, topic configs, connector types, and schema subjects; **generate** Aiven Terraform and MirrorMaker 2 config; and **document** role mapping, connector mapping, config limits, and credential migration.

---

#### Confluent proprietary / cloud-only connectors and Aiven alternatives

Confluent Cloud offers many fully-managed connectors; some are Confluent-managed variants of open-source connectors, others are cloud-only. Below: Confluent connectors that are **proprietary, cloud-only, or have different availability** on Aiven, and **suggested Aiven alternatives** (prefer Aiven-supported options such as Debezium where applicable).

| Confluent Cloud connector (examples) | Aiven alternative / notes |
|--------------------------------------|---------------------------|
| **Datagen Source** | **No direct equivalent.** Dev/test data generator. On Aiven: use custom producer, sample data, or request a similar plugin via [Aiven Ideas](https://ideas.aiven.io/). |
| **Salesforce** (PushTopic, SObject, CDC, Bulk API) | **Not in Aiven’s pre-approved connector list.** Options: (1) Request via Aiven Ideas; (2) Self-host Kafka Connect with a community Salesforce connector if license allows; (3) Use Aiven HTTP connector or custom app to integrate with Salesforce APIs. |
| **Confluent S3 (managed)** | **Aiven:** [Amazon S3 source](https://github.com/Aiven-Open/cloud-storage-connectors-for-apache-kafka), [S3 sink](https://aiven.io/docs/products/kafka/kafka-connect/howto/s3-sink-connector-aiven), [S3 IAM Assume Role](https://aiven.io/docs/products/kafka/kafka-connect/howto/s3-iam-assume-role). Direct alternative. |
| **Debezium MySQL / PostgreSQL / SQL Server / Oracle / MongoDB** | **Aiven:** Debezium for [PostgreSQL](https://aiven.io/docs/products/kafka/kafka-connect/howto/debezium-source-connector-pg), [MySQL](https://debezium.io/docs/connectors/mysql/), [SQL Server](https://debezium.io/docs/connectors/sqlserver/), [Oracle](https://aiven.io/docs/products/kafka/kafka-connect/howto/debezium-source-connector-oracle), [MongoDB](https://debezium.io/docs/connectors/mongodb/) (and TLS/node-replacement variants). Same family; config may differ. Ensure required Debezium features are enabled on Aiven. |
| **Snowflake** | **Aiven:** [Snowflake connector](https://docs.snowflake.com/en/user-guide/kafka-connector) (Snowflake’s own). Available; config and version may differ. |
| **Couchbase** | **Aiven:** [Couchbase](https://github.com/couchbase/kafka-connect-couchbase) (community). Same connector family. |
| **ClickHouse Sink** | **Aiven:** [ClickHouse sink](https://github.com/ClickHouse/clickhouse-kafka-connect). Direct alternative. |
| **Google BigQuery** | **Aiven:** [Google BigQuery](https://github.com/confluentinc/kafka-connect-bigquery) (Confluent’s open-source connector). Available on Aiven. |
| **Azure Synapse / Cosmos DB, ServiceNow, Zendesk, etc.** | Check [Aiven connector list](https://aiven.io/docs/products/kafka/kafka-connect/concepts/list-of-connector-plugins); many have JDBC, HTTP, or Stream Reactor equivalents. Request missing ones via Aiven Ideas. |

**Recommendation:** During discovery, list Confluent connector types in use; in create-asset or a companion doc, output a **connector mapping table** (Confluent → Aiven equivalent or “manual / request / N/A”) so operators know what to reconfigure or replace.

---

#### Topic and cluster configs with migration risk

These Kafka/topic-level configs can differ between Confluent Cloud and Aiven (defaults, limits, or support). During discovery, record them; during migration, validate or override so Confluent-reliant settings do not break on Aiven.

| Config | Confluent Cloud | Aiven / risk |
|--------|------------------|--------------|
| **max.message.bytes** (topic) / **message.max.bytes** (broker) | Up to 10 MB (cluster-type dependent). Connector `max.request.size` e.g. 8 MB (non-Dedicated), 20 MB (Dedicated). | Aiven: configurable via `kafka.message_max_bytes`; default often 1 MB. **Risk:** Larger messages on Confluent may fail or truncate on Aiven if not increased. |
| **num.partitions** | Set at topic creation; cluster partition limit applies. | Aiven: no hard cap in config; recommend &lt;4k partitions per broker, &lt;200k per cluster. **Risk:** Exceeding Aiven recommendations can impact performance. |
| **retention.ms** | Per-topic; can be unlimited with compaction. | Aiven: configurable; tiered storage and retention config available. **Risk:** Very large retention or different semantics may need adjustment. |
| **min.insync.replicas** | Configurable. | Aiven: configurable. **Risk:** Low if set explicitly; defaults can differ. |
| **compression.type** | producer, gzip, snappy, lz4, zstd, etc. | Aiven: same. **Risk:** Low. |
| **cleanup.policy** | delete, compact, delete+compact. | Aiven: same. **Risk:** Low. |
| **Partition count (cluster)** | Cluster-type limit (e.g. total partitions per cluster). | Aiven: recommend &lt;200k per cluster, &lt;7k topics. **Risk:** Large Confluent clusters may need topic/partition consolidation or multiple Aiven services. |
| **Request size (connector/client)** | Confluent: 8 MB or 20 MB depending on tier. | Aiven: align with `message.max.bytes` and connector config. **Risk:** Connector configs that assume Confluent’s higher request size may need tuning. |

**Mitigation:** Discovery outputs topic-level configs and partition counts; create-asset or docs emit **warnings** when `max.message.bytes` &gt; 1 MB, partition count &gt; 4k per broker or &gt; 200k per cluster, or topic count &gt; 7k, and suggest Aiven-side overrides where applicable.

---

#### Confluent Cloud vs Aiven: credentials and authentication

| Aspect | Confluent Cloud | Aiven for Kafka |
|--------|-----------------|-----------------|
| **Kafka client auth** | **SASL_SSL** with **PLAIN** (or SASL_OAUTHBEARER with OAuth/OIDC). Username = **API Key ID**, password = **API Secret**. TLS 1.2 required. | **SASL** over SSL: **PLAIN**, **SCRAM-SHA-256**, or **SCRAM-SHA-512** (default). Alternatively certificate-based auth. Set `kafka_authentication_methods.sasl` (or cert) via Console, CLI, API, or Terraform. |
| **Credential type** | **API keys** (long-lived): Key ID + Secret. Tied to resource (cluster, Schema Registry, etc.). Secrets may have `cflt` prefix (new keys). | **Service credentials**: username + password per service user. Generated per Aiven Kafka service; used for SASL (PLAIN or SCRAM). |
| **Where to get** | Confluent Cloud Console → API keys (cluster, Schema Registry, ksqlDB, etc.) or Cloud API. | Aiven Console → service → “Users” / credentials; or Aiven API/CLI. |
| **Rotation** | Rotate API key/secret in Console; update clients. | Regenerate or add users in Aiven; update clients. |
| **Schema Registry auth** | Same API key (cluster-scoped) or dedicated SR API key. | Service user credentials or dedicated SR credentials depending on Aiven service setup. |
| **MirrorMaker 2 source (Confluent)** | Use Confluent cluster API key as SASL username, secret as password; security protocol **SASL_SSL**. | N/A (Aiven is target). |
| **MirrorMaker 2 target (Aiven)** | N/A (Confluent is source). | Use Aiven Kafka service credentials (SASL PLAIN or SCRAM-SHA-512) for the MirrorMaker 2 integration to Aiven Kafka. |

**References:** [Confluent Cloud API keys](https://docs.confluent.io/cloud/current/security/authenticate/workload-identities/service-accounts/api-keys/overview.html), [Confluent client config](https://docs.confluent.io/cloud/current/client-apps/config-client.html), [Aiven SASL auth](https://aiven.io/docs/products/kafka/howto/kafka-sasl-auth).

---

#### Instance and plan differences (Confluent Cloud vs Aiven)

Confluent Cloud and Aiven use **different sizing and plan models**. There is **no 1:1 mapping** (e.g. “1 Confluent Dedicated cluster = 1 Aiven Business-8”). Migration requires **right-sizing on Aiven** based on workload (throughput, partition count, storage, features), not on Confluent tier names.

| Aspect | Confluent Cloud | Aiven for Kafka |
|--------|-----------------|-----------------|
| **Sizing model** | **Cluster types:** Basic, Standard, Enterprise, Dedicated, Freight. **Dedicated** is provisioned in **CKUs** (Confluent Kafka Units). Basic/Standard/Enterprise are usage-based. | **Plan tiers:** Free, Startup, Business (e.g. Business-4/8/16), Premium, Custom. **Fixed** node count, CPU, RAM, and storage per plan. **Classic Kafka** (local broker storage) vs **Inkless Kafka** (diskless, object storage). |
| **Capacity unit** | CKU (Dedicated); or pay-as-you-go by usage (other types). | **Nodes × (CPU, RAM, storage)** per plan. E.g. Business-8: 3 nodes, 4 CPU + 8 GB RAM per VM, 1,200 GB total. Premium: 6–30 nodes, larger storage. |
| **Partition / topic limits** | **Per cluster type** (documented in [Kafka Cluster Types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)); e.g. Dedicated has higher partition limits. Request size: 8 MB (Basic/Standard/Enterprise/Freight), 20 MB (Dedicated). | **Recommendations**, not hard product limits: &lt;4,000 partitions per broker, &lt;200,000 per cluster, &lt;7,000 topics. Configurable `message.max.bytes` (default often 1 MB). Smaller plans (e.g. 2 CPU) may need fewer partitions to avoid resource alerts. |
| **Billing** | **Consumption-based:** data transfer, request throughput, storage; Dedicated = fixed CKU cost. Hourly accrual. | **All-inclusive** per plan (monthly): compute, storage, networking in one price. Free tier for trial; Startup from ~$200/mo; Business from ~$500/mo; Premium from ~$1,900/mo; Custom from ~$5,000/mo. |
| **Connect / MirrorMaker** | Connectors and MirrorMaker are separate billed services (e.g. $/task/hour, $/GB). | **Kafka Connect** included in same cluster on Business+ plans. **MirrorMaker 2** is a **separate Aiven service** (own plan/cost). |
| **Private networking** | Dedicated/Enterprise: Private Link, VPC peering. | Available; depends on plan and cloud (AWS, GCP, Azure, etc.). |
| **Resize / scale** | Dedicated: add/remove CKUs (e.g. 30–60 min per CKU). Basic can upgrade to Standard (no downgrade). | **Vertical** (bigger nodes) and **horizontal** (more brokers) with near-zero downtime. Plan change or custom plan for large scale. |

**Risks from plan/instance differences**

- **Right-sizing:** A “Dedicated 4 CKU” or “Standard multi-zone” workload does not map to a single Aiven plan name. Over-provisioning or under-provisioning can cause cost or performance issues.
- **Partition/topic load:** Confluent’s per-tier partition limits may allow more partitions than Aiven’s recommendations for a given plan. Migrating without reducing partitions or choosing a larger Aiven plan can lead to resource alerts or throttling.
- **Message size:** Confluent allows up to 10 MB and 8/20 MB request size by tier; Aiven defaults can be lower. Large-message workloads need Aiven config and possibly a plan with enough headroom.
- **Cost comparison:** Confluent’s usage-based + connector/MirrorMaker billing is not directly comparable to Aiven’s fixed plan + separate MirrorMaker service. Migration planning should compare **actual Confluent usage/cost** to **candidate Aiven plan(s)** for similar workload.

**Mitigation**

1. **Discovery:** For Confluent source, record **cluster type**, **CKU** (if Dedicated), **topic count**, **partition count**, **total storage**, and **connector/MirrorMaker usage** so we can reason about equivalent Aiven capacity.
2. **Sizing guidance (doc or tool):** Provide a **mapping guide** (not automatic): e.g. “For &lt;N partitions and &lt;X GB storage and Connect, consider Aiven Business-8 or Premium” with links to [Aiven Kafka plan finder](https://aiven.io/kafka-plan-finder) and [Confluent cluster types](https://docs.confluent.io/cloud/current/clusters/cluster-types.html). Do not imply a single “Confluent tier → Aiven plan” table; recommend “match by workload.”
3. **Warnings in create-asset:** If discovered partition count or topic count exceeds Aiven recommendations for a “typical” plan, warn that the customer may need a larger plan, Inkless Kafka, or partition/topic consolidation.
4. **MirrorMaker 2:** Clarify in docs that Aiven MirrorMaker 2 is a **separate service** with its own plan; migration cost = Kafka cluster plan + MirrorMaker 2 plan (and optionally Connect on the Kafka plan).

---

## References

- [Confluent Cloud API](https://docs.confluent.io/cloud/current/api.html)
- [Aiven Terraform provider](https://registry.terraform.io/providers/aiven/aiven/latest/docs)
- [Aiven: Integrate external Kafka cluster](https://aiven.io/docs/products/kafka/howto/integrate-external-kafka-cluster)
- [Aiven: MirrorMaker 2 cross-cluster](https://aiven.io/developer/kafka-mirrormaker-crosscluster)
- [Aiven for Apache Kafka® governance](https://aiven.io/docs/products/kafka/concepts/governance-overview)
- [Aiven: Available Kafka Connect connectors](https://aiven.io/docs/products/kafka/kafka-connect/concepts/list-of-connector-plugins)
- [Confluent Cloud fully-managed connectors](https://docs.confluent.io/cloud/current/connectors/overview.html)
- [DataHub Kafka ingestion](https://docs.datahub.com/docs/0.15.0/generated/ingestion/sources/kafka/)
- [Kafka Cluster Types in Confluent Cloud](https://docs.confluent.io/cloud/current/clusters/cluster-types.html)
- [Aiven Kafka plan finder](https://aiven.io/kafka-plan-finder)

---

# Critical review: weaknesses, risks, and alternatives

## 1. Weaknesses in the original plan

### 1.1 State model Option B understates blast radius

The plan recommends extending `State` with `SourceType` and `ConfluentClusters` and “branching on SourceType” in report and create-asset. In reality, **every consumer of State and ProcessedState** assumes AWS/MSK shape:

- **Report service** (`ProcessState`, `FilterRegionCosts`, `FilterClusterMetrics`): iterates `state.Regions`, expects `ProcessedRegion` with AWS-specific `ProcessedRegionCosts` (e.g. `AWSCertificateManager`, `AmazonManagedStreamingForApacheKafka`, `EC2Other`), and `ProcessedCluster` with `AWSClientInformation` and `cluster.Arn` (MSK ARN).
- **UI API**: multiple handlers iterate `state.Regions` and use `cluster.Arn`, `cluster.AWSClientInformation` (e.g. target cluster lookup for migrate-ACLs, migrate-topics).
- **Frontend**: `clusterUtils.ts` uses `cluster.aws_client_information?.msk_cluster_config?.ClusterArn`; cost hooks expect `regions` with AWS cost structure.
- **create-asset**: `GetClusterByArn`, `utils.GetClusterByArn`, and many commands expect `cluster.AWSClientInformation` (MskClusterConfig, ClusterNetworking, Connectors, etc.).

So “branch on SourceType” means **adding branches or fallbacks in many places**, not just one or two. Confluent-only state would have **empty `Regions`**, which already breaks:

- `cmd/report/costs/cmd_report_costs.go` and `cmd_report_metrics.go`: `if len(state.Regions) == 0` → error.
- `State.WriteReportCommands`: writes only region/cluster commands for `s.Regions`; Confluent state would produce an empty or useless file.
- `NewStateFrom(fromState)`: copies `fromState.Regions`; no handling for Confluent clusters.

**Mitigation:** Either (a) map Confluent clusters into a **virtual region** (e.g. one “region” per environment) and a **unified cluster type** that can hold either AWS or Confluent fields (with lots of optional/nil handling), or (b) use a **separate state shape and pipeline** (see “Better alternative” below).

### 1.2 ProcessedState is AWS-shaped, not generic

`ProcessedState` and related types are not source-agnostic:

- `ProcessedRegionCosts` / `ProcessedAggregates`: AWS service names (e.g. `Amazon Managed Streaming for Apache Kafka`).
- `ProcessedCluster` embeds `AWSClientInformation` (MSK config, bootstrap brokers, etc.).
- Report service flattens **AWS Cost Explorer** and **CloudWatch** structures.

So “skip or replace cost/metrics for Confluent” is not a small change: the **processed** model is AWS-specific. The UI and report API return this shape; the frontend expects it. Either we add a parallel `ProcessedStateConfluent` (and duplicate or branch report/UI logic), or we design a **unified processed model** that can represent both (e.g. optional cost/metrics, optional AWS vs Confluent cluster payload). The plan did not detail this.

### 1.3 Credentials and auth flow are MSK-specific

Today, discovery and scan use `cluster-credentials.yaml` with a **region → cluster → auth** structure (SASL/SCRAM, IAM, etc.) tailored to MSK. Confluent Cloud uses **API key + secret** (and possibly different scopes: org vs cluster). The plan mentions “Auth: API key + secret” but does not specify:

- Where Confluent credentials are stored (same file with a new format? env vars? separate file?).
- How `scan` and create-asset would look up Confluent credentials (current code paths are keyed by region + cluster ARN).

Without a clear credential story, Confluent discovery and “scan Confluent clusters” are incomplete.

### 1.4 Confluent Cloud API: rate limits and auth scope

The plan does not mention:

- **Rate limits**: Confluent Cloud REST APIs may throttle (e.g. 429). Discovery of many clusters/topics could hit limits; we need **retries and backoff** in the Confluent client.
- **Auth scope**: Kafka cluster API keys vs Cloud/org-level API keys may differ. We need to confirm which API key type is required for listing environments/clusters and for Kafka REST (topics, ACLs) and document it.

### 1.5 Aiven replication: ACLs and operational details

The plan says “ensure auth params are passed through” but does not call out:

- **ACL requirements**: Aiven docs require specific ACLs on both source (Confluent) and target (Aiven) for MirrorMaker 2 (e.g. READ on source topics, WRITE on target, internal topics). We should generate or document required ACLs and, if possible, emit Terraform/scripts for them.
- **SASL_SSL**: Confluent Cloud requires SASL_SSL; the generated Aiven external-cluster / MirrorMaker config must use SASL_SSL, not SASL_PLAINTEXT.

### 1.6 Testing and CI

The plan does not address:

- **Testing Confluent discovery** without a live Confluent Cloud account: we need **mocked Confluent API responses** or a **contract test** against a real API (with credentials in CI secrets).
- **Testing Aiven target generation**: Terraform plan or unit tests with mocked Aiven provider / HCL output.

Without this, regressions and compatibility (e.g. Confluent API changes) are hard to catch.

**Approach:** Use **mocked Confluent API responses** for Confluent discovery and related tests for now. We may add **live-API testing** (e.g. contract or integration tests against Confluent Cloud with credentials in CI secrets) later.

### 1.7 UI: hardcoded MSK and Confluent Cloud branding

The frontend has strings like “Migrate your Kafka clusters to Confluent Cloud”. For a Confluent → Aiven flow, the UI should reflect the **target** (e.g. “Migrate to Aiven”) and not assume Confluent as destination. The plan mentions “ensure UI can load and display Confluent-sourced state” but not **copy and wording** or **flow-specific views**.

---

## 2. Caveats and risks not fully mitigated

| Risk | Impact | Mitigation (additional) |
|------|--------|-------------------------|
| **Empty `Regions` for Confluent state** | Report commands, cost/metrics report, and any code that assumes `len(Regions) > 0` can fail or produce empty output. | Define behavior: either virtual regions for Confluent, or separate state path that never touches `Regions`. |
| **Frontend expects MSK cluster shape** | `getClusterArn` and any UI that uses `aws_client_information.msk_cluster_config` break for Confluent clusters. | Introduce a stable cluster identifier (e.g. `id` or `cluster_id`) and use it when present; fall back to MSK ARN for backward compatibility. |
| **Two state shapes in one codebase** | Long-term maintenance burden and risk of bugs when one path is rarely used. | Prefer a clear separation (see “Better alternative”) and good tests for both paths. |
| **Confluent API changes or deprecations** | Discovery or scan could break after Confluent API updates. | Pin or document supported API version; add integration or contract tests; handle errors and log API versions. |
| **Aiven Terraform provider changes** | Generated HCL might become invalid after provider upgrades. | Pin provider version in generated Terraform; document supported provider version; consider validation (e.g. `terraform validate`) in CI. |

---

## 3. Better alternative: separate pipeline for Confluent → Aiven

Instead of overloading the existing `State` and branching everywhere, use a **separate pipeline** for the Confluent → Aiven flow. This reduces risk to the existing MSK → Confluent flow and keeps the new feature set contained.

### 3.1 Separate state shape and commands

- **State:** Introduce a **dedicated state type** for Confluent-sourced discovery, e.g. `ConfluentMigrationState` or a top-level `"source_type": "confluent_cloud"` with a **different** structure (e.g. `environments` / `confluent_clusters` instead of `regions`). Optionally use the same file name (`kcp-state.json`) but with a **discriminator** so the loader can return either `State` (MSK) or `ConfluentMigrationState`.
- **Discover:** Keep `kcp discover` for MSK only. Add **`kcp discover-confluent`** (or `kcp discover --source confluent` that delegates to a separate implementation) that writes the Confluent state shape. No changes to existing `State` or `NewStateFrom` for MSK.
- **Report:** Add **`kcp report topology`** (or `report confluent-topology`) that reads Confluent state and outputs a topology report (clusters, topics, no AWS cost/metrics). No change to existing `report costs` / `report metrics` (they continue to require MSK state with regions).
- **Create-asset:** Add **`kcp create-asset target-infra-aiven`** and **`kcp create-asset migrate-topics-aiven`** (or `--target aiven` that only accepts Confluent state) that read **Confluent state only** and generate Aiven Terraform + MirrorMaker 2 config. They do not accept MSK state; no branching inside existing target-infra or migrate-topics for MSK.

### 3.2 Benefits

- **No changes to existing State, ProcessedState, or report service** for the MSK path; no risk of breaking current users.
- **UI:** Can add a “Confluent → Aiven” flow that loads Confluent state and calls the new create-asset endpoints; existing UI continues to work with MSK state only.
- **Clear boundaries:** Confluent discovery, Confluent state, and Aiven target code live in separate packages; shared code (e.g. HCL helpers, topic list handling) can be factored into internal packages used by both pipelines.
- **Easier testing:** Mock Confluent API and Aiven HCL generation in isolation; no need to mock AWS and Confluent in the same flow.

### 3.3 Trade-offs

- **Two state file “formats”:** Operators must run the right discover command for the flow they want. Documentation and CLI help should make this explicit.
- **Some duplication:** E.g. “list topics” might exist for both MSK (scan) and Confluent (discover-confluent). Acceptable if we keep shared logic in a small set of helpers.
- **UI:** Either two entry points (upload MSK state vs upload Confluent state) or one upload that detects state type and switches view. Both are feasible.

### 3.4 Recommended implementation order (revised)

1. **Confluent Cloud client** (with retries, backoff, and documented auth scope).
2. **Confluent state type and `discover-confluent`** (writes Confluent state only; no change to `State`).
3. **Report topology for Confluent** (reads Confluent state; optional, for visibility).
4. **Aiven target infra** (`create-asset target-infra-aiven`) and **Confluent → Aiven replication** (MirrorMaker 2 / external cluster; `create-asset migrate-topics-aiven`).
5. **Topics/schemas/ACLs** for Aiven (extend create-asset for Aiven target).
6. **UI support** for Confluent state (new or detected flow) and wording for “Migrate to Aiven”.
7. **Credentials:** Define and implement Confluent credential storage and lookup.
8. **Tests:** **Mocked Confluent API responses** for Confluent discovery and related tests (primary approach). Optionally add **live-API testing** (contract/integration tests against Confluent Cloud) later. Terraform validate or unit tests for generated Aiven HCL; document ACL and SASL_SSL requirements.

This revision keeps the original plan’s goals but reduces risk by **not** overloading the existing State and report/create-asset paths, and by making the Confluent → Aiven path a **first-class but separate** pipeline.

---

## 4. How we address each weakness, risk, and caveat

Below is an explicit mapping: for each weakness/risk from sections 1–2, the plan we have to address it.

| # | Weakness / risk | How we address it |
|---|----------------------------------|-------------------|
| 1.1 | State Option B blast radius; empty `Regions` | **Separate state shape** (Section 3.1): `ConfluentMigrationState` (or discriminator) with its own structure. Confluent pipeline never touches `State.Regions`; MSK path unchanged. |
| 1.2 | ProcessedState is AWS-shaped | **Separate pipeline**: Confluent flow uses Confluent state only. Report for Confluent is **topology-only** (`report topology`); no need to extend ProcessedState or add ProcessedStateConfluent for cost/metrics. |
| 1.3 | Credentials MSK-specific; no Confluent credential story | **Revised order step 7**: “Credentials: Define and implement Confluent credential storage and lookup.” Design where Confluent API key/secret live and how discover-confluent and create-asset look them up. |
| 1.4 | Confluent API rate limits and auth scope | **Revised order step 1**: “Confluent Cloud client (with **retries, backoff**, and **documented auth scope**).” Implement and document in the client. |
| 1.5 | Aiven replication ACLs and SASL_SSL | **Revised order step 4** (replication) + **step 8**: Generate/config must use SASL_SSL for Confluent; step 8 “**document ACL and SASL_SSL requirements**” and, in implementation, generate or document required ACLs for MirrorMaker 2. |
| 1.6 | No testing strategy | **Revised order step 8**: **Mocked Confluent API** responses for now; **live-API testing** may be added later. Terraform validate or unit tests for Aiven HCL. |
| 1.7 | UI copy assumes Confluent as target | **Revised order step 6**: “UI support for Confluent state … and **wording for ‘Migrate to Aiven’**.” |
| 2 | Risk: empty Regions | Addressed by separate state: Confluent state has no `Regions`; Confluent commands never call report costs/metrics that require Regions. |
| 2 | Risk: frontend expects MSK shape | Confluent UI flow uses Confluent state and Confluent-specific views; no need to change `getClusterArn` for MSK. Optional: later add a shared cluster identifier if we unify views. |
| 2 | Risk: two state shapes maintenance | **Section 3.2**: Clear boundaries, separate packages, tests for both paths. |
| 2 | Risk: Confluent API / Aiven provider changes | **Step 1** (document auth/version); **step 8** (mocked tests now; optional live-API/contract tests later; Terraform validate); document supported API/provider versions. |

With the **better alternative** (Section 3) and this mapping, we have explicit plans to address every listed weakness, risk, and caveat.
