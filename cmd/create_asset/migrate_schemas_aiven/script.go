package migrate_schemas_aiven

// exportImportScript is a bash script that exports schemas from Confluent Schema Registry
// and imports them into Aiven Karapace. Uses env vars for URLs and auth (no secrets in repo).
const exportImportScript = `#!/usr/bin/env bash
set -euo pipefail

# Export schemas from Confluent Schema Registry and import into Aiven (Karapace).
# Required env: CONFLUENT_SR_URL, AIVEN_SR_URL
# Auth: CONFLUENT_SR_BASIC_AUTH or CONFLUENT_SR_USER+CONFLUENT_SR_PASSWORD;
#       AIVEN_SR_ACCESS_TOKEN or AIVEN_SR_BASIC_AUTH

CONFLUENT_SR_URL="${CONFLUENT_SR_URL:-}"
AIVEN_SR_URL="${AIVEN_SR_URL:-}"
CONFLUENT_SR_BASIC_AUTH="${CONFLUENT_SR_BASIC_AUTH:-}"
CONFLUENT_SR_USER="${CONFLUENT_SR_USER:-}"
CONFLUENT_SR_PASSWORD="${CONFLUENT_SR_PASSWORD:-}"
AIVEN_SR_ACCESS_TOKEN="${AIVEN_SR_ACCESS_TOKEN:-}"
AIVEN_SR_BASIC_AUTH="${AIVEN_SR_BASIC_AUTH:-}"
SUBJECTS_FILTER="${SUBJECTS:-}"  # optional comma-separated list

if [[ -z "$CONFLUENT_SR_URL" ]]; then
  echo "ERROR: CONFLUENT_SR_URL is required" >&2
  exit 1
fi
if [[ -z "$AIVEN_SR_URL" ]]; then
  echo "ERROR: AIVEN_SR_URL is required" >&2
  exit 1
fi

# Build Confluent auth header
CONFLUENT_AUTH_HEADER=""
if [[ -n "$CONFLUENT_SR_BASIC_AUTH" ]]; then
  CONFLUENT_AUTH_HEADER="Authorization: Basic $CONFLUENT_SR_BASIC_AUTH"
elif [[ -n "$CONFLUENT_SR_USER" && -n "$CONFLUENT_SR_PASSWORD" ]]; then
  CONFLUENT_AUTH_HEADER="Authorization: Basic $(echo -n "${CONFLUENT_SR_USER}:${CONFLUENT_SR_PASSWORD}" | base64)"
fi

# Build Aiven auth header
AIVEN_AUTH_HEADER=""
if [[ -n "$AIVEN_SR_ACCESS_TOKEN" ]]; then
  AIVEN_AUTH_HEADER="Authorization: Bearer $AIVEN_SR_ACCESS_TOKEN"
elif [[ -n "$AIVEN_SR_BASIC_AUTH" ]]; then
  AIVEN_AUTH_HEADER="Authorization: Basic $AIVEN_SR_BASIC_AUTH"
fi

confluent_curl() {
  local url="$1"
  local extra_args=("${@:2}")
  local auth=()
  if [[ -n "$CONFLUENT_AUTH_HEADER" ]]; then
    auth=(-H "$CONFLUENT_AUTH_HEADER")
  fi
  curl -sf "${auth[@]}" -H "Content-Type: application/vnd.schemaregistry.v1+json" "${extra_args[@]}" "$url"
}

aiven_curl() {
  local method="$1"
  local url="$2"
  local data="${3:-}"
  local auth=()
  if [[ -n "$AIVEN_AUTH_HEADER" ]]; then
    auth=(-H "$AIVEN_AUTH_HEADER")
  fi
  if [[ -n "$data" ]]; then
    curl -sf -X "$method" "${auth[@]}" -H "Content-Type: application/vnd.schemaregistry.v1+json" -d "$data" "$url"
  else
    curl -sf -X "$method" "${auth[@]}" -H "Content-Type: application/vnd.schemaregistry.v1+json" "$url"
  fi
}

# Normalize URL (no trailing slash)
CONFLUENT_SR_URL="${CONFLUENT_SR_URL%/}"
AIVEN_SR_URL="${AIVEN_SR_URL%/}"

# Resolve subject list
if [[ -n "$SUBJECTS_FILTER" ]]; then
  SUBJECTS_JSON="["
  first=true
  while IFS=',' read -ra S; do
    for s in "${S[@]}"; do
      s=$(echo "$s" | tr -d ' ')
      [[ -z "$s" ]] && continue
      if [[ "$first" == true ]]; then first=false; else SUBJECTS_JSON+=","; fi
      SUBJECTS_JSON+="\"$s\""
    done
  done <<< "$SUBJECTS_FILTER"
  SUBJECTS_JSON+="]"
else
  SUBJECTS_JSON=$(confluent_curl "${CONFLUENT_SR_URL}/subjects") || { echo "Failed to list Confluent subjects" >&2; exit 1; }
fi

count=0
echo "$SUBJECTS_JSON" | jq -r '.[]' 2>/dev/null | while read -r subject; do
  [[ -z "$subject" ]] && continue
  echo "Migrating subject: $subject"
  schema_payload=$(confluent_curl "${CONFLUENT_SR_URL}/subjects/${subject}/versions/latest") || { echo "  Failed to get latest version" >&2; continue; }
  # Karapace accepts same format: {"schema": "...", "schemaType": "AVRO"} etc.
  body=$(echo "$schema_payload" | jq -c '{schema: .schema, schemaType: (.schemaType // "AVRO")}')
  aiven_curl POST "${AIVEN_SR_URL}/subjects/${subject}/versions" "$body" >/dev/null || { echo "  Failed to create version on Aiven" >&2; continue; }
  echo "  OK"
  ((count++)) || true
done

echo "Done."
`
