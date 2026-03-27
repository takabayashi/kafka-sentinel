#!/usr/bin/env bash
# ============================================================================
# Simplified Platform Health Check
# Uses Terraform outputs and minimal API calls
# ============================================================================

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/terraform"

print_header() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

main() {
    echo ""
    echo -e "${BLUE}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║                                                        ║${NC}"
    echo -e "${BLUE}║         Kafka Sentinel Platform Health Check          ║${NC}"
    echo -e "${BLUE}║                                                        ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════╝${NC}"

    cd "$TERRAFORM_DIR"

    # Check Terraform state
    print_header "Infrastructure Status (from Terraform)"

    if [[ ! -f "terraform.tfstate" ]]; then
        echo -e "  ${RED}❌ No Terraform state found. Run 'terraform apply' first.${NC}"
        exit 1
    fi

    # Get outputs
    echo "  Checking Terraform outputs..."
    FLINK_POOL=$(terraform output -raw flink_compute_pool_id 2>/dev/null || echo "")
    CLUSTER_ID=$(terraform output -raw cluster_id 2>/dev/null || echo "")
    ENV_ID=$(terraform output -raw environment_id 2>/dev/null || echo "")

    if [[ -z "$FLINK_POOL" ]]; then
        echo -e "  ${RED}❌ Flink compute pool not deployed${NC}"
    else
        echo -e "  ${GREEN}✅${NC} Flink Compute Pool: $FLINK_POOL"
    fi

    if [[ -z "$CLUSTER_ID" ]]; then
        echo -e "  ${RED}❌ Kafka cluster not configured${NC}"
    else
        echo -e "  ${GREEN}✅${NC} Kafka Cluster: $CLUSTER_ID"
    fi

    if [[ -z "$ENV_ID" ]]; then
        echo -e "  ${RED}❌ Environment not configured${NC}"
    else
        echo -e "  ${GREEN}✅${NC} Environment: $ENV_ID"
    fi

    # Check Flink Tables
    print_header "Flink Catalog Tables"

    FLINK_TABLES=$(terraform output -json flink_catalog_tables 2>/dev/null || echo "{}")

    if echo "$FLINK_TABLES" | jq -e '. | length > 0' >/dev/null 2>&1; then
        echo "$FLINK_TABLES" | jq -r 'to_entries[] | "  \u2705 \(.key): \(.value)"'
    else
        echo -e "  ${YELLOW}⚠️  No Flink catalog tables found${NC}"
    fi

    # Check Topics
    print_header "Kafka Topics"

    TOPICS=$(terraform output -json topic_names 2>/dev/null || echo "{}")

    if echo "$TOPICS" | jq -e '. | length > 0' >/dev/null 2>&1; then
        echo "$TOPICS" | jq -r 'to_entries[] | "  \u2705 \(.value)"'
    else
        echo -e "  ${YELLOW}⚠️  No topics found${NC}"
    fi

    # Service Account
    print_header "Service Account"

    SA_ID=$(terraform output -raw flink_service_account_id 2>/dev/null || echo "")

    if [[ -n "$SA_ID" ]]; then
        echo -e "  ${GREEN}✅${NC} Service Account: $SA_ID"
        echo "     Roles: FlinkDeveloper, DeveloperRead/Write (Kafka & Schema Registry)"
    else
        echo -e "  ${YELLOW}⚠️  No service account found${NC}"
    fi

    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}  Health check complete!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

main "$@"
