#!/bin/bash

# Fivetran Connector SDK Terraform Deployment Script
# This script automates the deployment of Connector SDK connectors using Terraform

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TF_DIR="$(pwd)"
CONNECTOR_DIR="/Users/elijah.davis/Documents/code/sdk/tests/sdk_sflk"
BACKUP_DIR="./backups/$(date +%Y%m%d_%H%M%S)"

# Fivetran Configuration
FIVETRAN_CSDK_API_KEY="${FIVETRAN_CSDK_API_KEY:-}"
FIVETRAN_DESTINATION="${FIVETRAN_DESTINATION:-}"
FIVETRAN_CONNECTION="${FIVETRAN_CONNECTION:-}"

echo -e "${BLUE}🚀 Fivetran Connector SDK Terraform Deployment${NC}"
echo "=================================================="

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to backup current state
backup_state() {
    if [ -f "terraform.tfstate" ]; then
        mkdir -p "$BACKUP_DIR"
        cp terraform.tfstate "$BACKUP_DIR/"
        cp terraform.tfstate.backup "$BACKUP_DIR/" 2>/dev/null || true
        print_status "State backed up to $BACKUP_DIR"
    fi
}

# Function to validate connector code
validate_connector() {
    print_status "Validating Connector SDK code..."
    
    if [ ! -f "$CONNECTOR_DIR/connector.py" ]; then
        print_error "Connector code not found at $CONNECTOR_DIR/connector.py"
        exit 1
    fi
    
    if [ ! -f "$CONNECTOR_DIR/requirements.txt" ]; then
        print_error "Requirements file not found at $CONNECTOR_DIR/requirements.txt"
        exit 1
    fi
    
    if [ ! -f "$CONNECTOR_DIR/configuration.json" ]; then
        print_error "Configuration file not found at $CONNECTOR_DIR/configuration.json"
        exit 1
    fi
    
    print_status "Connector files validated"
}

# Function to validate Fivetran CLI
validate_fivetran_cli() {
    print_status "Validating Fivetran CLI installation..."
    
    if ! command -v fivetran &> /dev/null; then
        print_error "Fivetran CLI is not installed. Please install it first."
        print_warning "Installation instructions: https://fivetran.com/docs/connectors/connector-sdk/getting-started"
        exit 1
    fi
    
    # Test Fivetran CLI version
    fivetran_version=$(fivetran --version 2>/dev/null || echo "unknown")
    print_status "Fivetran CLI version: $fivetran_version"
}

# Function to test connector locally
test_connector() {
    print_status "Testing Connector SDK locally..."
    
    cd "$CONNECTOR_DIR"
    
    # Install dependencies
    pip install -r requirements.txt
    pip install fivetran-connector-sdk
    
    # Test connector
    python -c "
import json
from connector import schema, update

# Load configuration
with open('configuration.json', 'r') as f:
    config = json.load(f)

# Test schema
try:
    schema_result = schema(config)
    print('✅ Schema test passed:', schema_result)
except Exception as e:
    print('❌ Schema test failed:', e)
    exit(1)


"
    
    cd "$TF_DIR"
    print_status "Local connector test completed"
}

# Function to deploy with Terraform
deploy_terraform() {
    print_status "Deploying with Terraform..."
    
    # Initialize Terraform
    terraform init
    
    # Validate Terraform configuration
    terraform validate
    
    # Plan deployment
    print_status "Planning Terraform deployment..."
    terraform plan -out=tfplan
    
    # Ask for confirmation
    echo -e "${YELLOW}Do you want to apply this plan? (y/N)${NC}"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        print_status "Applying Terraform plan..."
        terraform apply tfplan
        print_status "Terraform deployment completed successfully!"
    else
        print_warning "Deployment cancelled by user"
        rm -f tfplan
        exit 0
    fi
    
    # Clean up plan file
    rm -f tfplan
}

# Function to deploy to Fivetran
deploy_fivetran() {
    print_status "Deploying to Fivetran..."
    
    # Validate Fivetran CLI
    validate_fivetran_cli
    
    # Validate Fivetran configuration
    if [ -z "$FIVETRAN_CSDK_API_KEY" ]; then
        print_error "FIVETRAN_CSDK_API_KEY environment variable is required"
        print_warning "Set it with: export FIVETRAN_CSDK_API_KEY='your_base64_encoded_api_key'"
        exit 1
    fi
    
    if [ -z "$FIVETRAN_DESTINATION" ]; then
        print_error "FIVETRAN_DESTINATION environment variable is required"
        print_warning "Set it with: export FIVETRAN_DESTINATION='your_destination_name'"
        exit 1
    fi
    
    if [ -z "$FIVETRAN_CONNECTION" ]; then
        print_error "FIVETRAN_CONNECTION environment variable is required"
        print_warning "Set it with: export FIVETRAN_CONNECTION='your_connection_name'"
        exit 1
    fi
    
    # Check if configuration.json exists
    if [ ! -f "$CONNECTOR_DIR/configuration.json" ]; then
        print_error "Configuration file not found at $CONNECTOR_DIR/configuration.json"
        exit 1
    fi
    
    # Deploy to Fivetran
    cd "$CONNECTOR_DIR"
    print_status "Running Fivetran deploy command..."
    print_status "Destination: $FIVETRAN_DESTINATION"
    print_status "Connection: $FIVETRAN_CONNECTION"
    
    fivetran deploy \
        --api-key "$FIVETRAN_CSDK_API_KEY" \
        --destination "$FIVETRAN_DESTINATION" \
        --connection "$FIVETRAN_CONNECTION" \
        --configuration configuration.json
    
    if [ $? -eq 0 ]; then
        print_status "Fivetran deployment completed successfully!"
    else
        print_error "Fivetran deployment failed"
        cd "$TF_DIR"
        exit 1
    fi
    
    cd "$TF_DIR"
}

# Function to show deployment status
show_status() {
    print_status "Checking deployment status..."
    
    if [ -f "terraform.tfstate" ]; then
        echo -e "${BLUE}Current Terraform State:${NC}"
        terraform show -json | jq -r '.values.root_module.resources[] | "\(.type).\(.name): \(.values.id // "N/A")"'
    else
        print_warning "No Terraform state found"
    fi
    
    # Show Fivetran configuration if available
    if [ -n "$FIVETRAN_DESTINATION" ] && [ -n "$FIVETRAN_CONNECTION" ]; then
        echo -e "${BLUE}Fivetran Configuration:${NC}"
        echo "  Destination: $FIVETRAN_DESTINATION"
        echo "  Connection: $FIVETRAN_CONNECTION"
        echo "  API Key: ${FIVETRAN_CSDK_API_KEY:0:10}..."
    fi
}

# Function to check Fivetran connection status
check_fivetran_status() {
    print_status "Checking Fivetran connection status..."
    
    # Validate Fivetran CLI
    validate_fivetran_cli
    
    # Validate Fivetran configuration
    if [ -z "$FIVETRAN_CSDK_API_KEY" ] || [ -z "$FIVETRAN_DESTINATION" ] || [ -z "$FIVETRAN_CONNECTION" ]; then
        print_error "Fivetran environment variables not set. Cannot check status."
        exit 1
    fi
    
    cd "$CONNECTOR_DIR"
    
    # Check connection status
    print_status "Checking connection: $FIVETRAN_CONNECTION"
    fivetran status \
        --api-key "$FIVETRAN_CSDK_API_KEY" \
        --destination "$FIVETRAN_DESTINATION" \
        --connection "$FIVETRAN_CONNECTION"
    
    cd "$TF_DIR"
}

# Function to destroy resources (if needed)
destroy_resources() {
    print_warning "This will destroy all managed resources. Are you sure? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        print_status "Destroying resources..."
        terraform destroy -auto-approve
        print_status "Resources destroyed successfully"
    else
        print_warning "Destroy cancelled by user"
    fi
}

# Main execution
main() {
    case "${1:-deploy}" in
        "deploy")
            backup_state
            validate_connector
            test_connector
            deploy_fivetran
            deploy_terraform
            show_status
            ;;
        "deploy-terraform")
            backup_state
            validate_connector
            test_connector
            deploy_terraform
            show_status
            ;;
        "deploy-fivetran")
            validate_connector
            test_connector
            deploy_fivetran
            ;;
        "test")
            validate_connector
            test_connector
            ;;
        "plan")
            terraform init
            terraform validate
            terraform plan
            ;;
        "apply")
            terraform apply
            ;;
        # "destroy")
        #     destroy_resources
        #     ;;
        "status")
            show_status
            ;;
        "fivetran-status")
            check_fivetran_status
            ;;
        "backup")
            backup_state
            ;;
        *)
            echo "Usage: $0 {deploy|deploy-terraform|deploy-fivetran|test|plan|apply|destroy|status|fivetran-status|backup}"
            echo ""
            echo "Commands:"
            echo "  deploy         - Full deployment (backup, validate, test, fivetran, terraform)"
            echo "  deploy-terraform - Deploy only Terraform infrastructure"
            echo "  deploy-fivetran  - Deploy only to Fivetran"
            echo "  test           - Test connector locally"
            echo "  plan           - Show Terraform plan"
            echo "  apply          - Apply Terraform changes"
            echo "  destroy        - Destroy all resources"
            echo "  status         - Show current status"
            echo "  fivetran-status - Check Fivetran connection status"
            echo "  backup         - Backup current state"
            echo ""
            echo "Environment Variables:"
            echo "  FIVETRAN_CSDK_API_KEY      - Base64 encoded Fivetran API key"
            echo "  FIVETRAN_DESTINATION  - Fivetran destination name"
            echo "  FIVETRAN_CONNECTION   - Fivetran connection name"
            exit 1
            ;;
    esac
}

# Check if jq is installed for JSON parsing
if ! command -v jq &> /dev/null; then
    print_warning "jq is not installed. Install it for better output formatting."
fi

# Run main function
main "$@" 
