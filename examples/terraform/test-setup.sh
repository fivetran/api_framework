#!/bin/bash

# Test script for Fivetran Connector SDK CI/CD setup
# This script validates that all components are properly configured

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧪 Testing Fivetran Connector SDK CI/CD Setup${NC}"
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

# Test 1: Check if required tools are installed
echo -e "${BLUE}1. Checking required tools...${NC}"

# Check Terraform
if command -v terraform &> /dev/null; then
    TF_VERSION=$(terraform version -json | jq -r '.terraform_version')
    print_status "Terraform found: $TF_VERSION"
else
    print_error "Terraform not found. Please install Terraform v1.5.0+"
    exit 1
fi

# Check Python
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    print_status "Python found: $PYTHON_VERSION"
else
    print_error "Python3 not found. Please install Python 3.9+"
    exit 1
fi

# Check jq
if command -v jq &> /dev/null; then
    print_status "jq found for JSON parsing"
else
    print_warning "jq not found. Install for better output formatting"
fi

# Test 2: Check Terraform configuration
echo -e "${BLUE}2. Validating Terraform configuration...${NC}"

if [ -f "main.tf" ]; then
    print_status "main.tf found"
else
    print_error "main.tf not found"
    exit 1
fi

if [ -f "connections.tf" ]; then
    print_status "connections.tf found"
else
    print_error "connections.tf not found"
    exit 1
fi

if [ -f "terraform.tfvars" ]; then
    print_status "terraform.tfvars found"
else
    print_error "terraform.tfvars not found"
    exit 1
fi

# Test 3: Validate Terraform syntax
echo -e "${BLUE}3. Validating Terraform syntax...${NC}"

if terraform validate &> /dev/null; then
    print_status "Terraform configuration is valid"
else
    print_error "Terraform configuration has errors"
    terraform validate
    exit 1
fi

# Test 4: Check connector files
echo -e "${BLUE}4. Checking connector files...${NC}"

CONNECTOR_DIR="/Users/elijah.davis/Documents/code/sdk/tests/sdk_sflk"

if [ -f "$CONNECTOR_DIR/connector.py" ]; then
    print_status "connector.py found"
else
    print_error "connector.py not found at $CONNECTOR_DIR/connector.py"
    exit 1
fi

if [ -f "$CONNECTOR_DIR/requirements.txt" ]; then
    print_status "requirements.txt found"
else
    print_error "requirements.txt not found"
    exit 1
fi

if [ -f "$CONNECTOR_DIR/configuration.json" ]; then
    print_status "configuration.json found"
else
    print_error "configuration.json not found"
    exit 1
fi

# Test 5: Check GitHub Actions workflow
echo -e "${BLUE}5. Checking CI/CD pipeline...${NC}"

if [ -f ".github/workflows/terraform-cicd.yml" ]; then
    print_status "GitHub Actions workflow found"
else
    print_warning "GitHub Actions workflow not found at .github/workflows/terraform-cicd.yml"
fi

# Test 6: Check deployment script
echo -e "${BLUE}6. Checking deployment script...${NC}"

if [ -f "deploy.sh" ]; then
    if [ -x "deploy.sh" ]; then
        print_status "deploy.sh found and executable"
    else
        print_warning "deploy.sh found but not executable. Run: chmod +x deploy.sh"
    fi
else
    print_error "deploy.sh not found"
    exit 1
fi

# Test 7: Validate configuration.json format
echo -e "${BLUE}7. Validating configuration.json...${NC}"

if python3 -c "
import json
import sys

try:
    with open('$CONNECTOR_DIR/configuration.json', 'r') as f:
        config = json.load(f)
    
    required_keys = [
        'fivetran_csdk_api_key', 'fivetran_api_secret', 'fivetran_connector_id',
        'freshness_query'
    ]
    
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        print(f'Missing required keys: {missing_keys}')
        sys.exit(1)
    else:
        print('All required configuration keys present')
        sys.exit(0)
        
except Exception as e:
    print(f'Error validating configuration.json: {e}')
    sys.exit(1)
"; then
    print_status "configuration.json is valid"
else
    print_error "configuration.json has issues"
    exit 1
fi

# Test 8: Check Python dependencies
echo -e "${BLUE}8. Checking Python dependencies...${NC}"

cd "$CONNECTOR_DIR"

if python3 -c "
import sys
import pkg_resources

required_packages = [
    'snowflake-connector-python',
    'requests'
]

missing_packages = []

for package in required_packages:
    try:
        pkg_resources.require(package)
    except pkg_resources.DistributionNotFound:
        missing_packages.append(package)

if missing_packages:
    print(f'Missing packages: {missing_packages}')
    print('Run: pip install -r requirements.txt')
    sys.exit(1)
else:
    print('All required Python packages available')
    sys.exit(0)
"; then
    print_status "Python dependencies are available"
else
    print_warning "Some Python packages may be missing. Run: pip install -r requirements.txt"
fi

cd - > /dev/null

# Test 9: Check for sensitive data exposure
echo -e "${BLUE}9. Checking for sensitive data exposure...${NC}"

# Check if terraform.tfvars contains actual secrets (not placeholders)
if grep -q "sensitive_data" terraform.tfvars; then
    print_warning "terraform.tfvars contains actual API keys. Consider using environment variables for CI/CD"
else
    print_status "No hardcoded secrets found in terraform.tfvars"
fi

# Check if .gitignore is properly configured
if [ -f ".gitignore" ]; then
    if grep -q "*.tfstate" .gitignore; then
        print_status ".gitignore properly configured for Terraform"
    else
        print_warning ".gitignore may not be properly configured"
    fi
else
    print_warning ".gitignore not found"
fi

# Summary
echo -e "${BLUE}=================================================="
echo -e "${GREEN}🎉 Setup validation completed!${NC}"
echo -e "${BLUE}=================================================="

echo -e "${YELLOW}Next steps:${NC}"
echo "1. Set up GitHub Secrets for CI/CD"
echo "2. Test connector locally: ./deploy.sh test"
echo "3. Deploy to Fivetran: ./deploy.sh deploy"
echo "4. Monitor deployment in GitHub Actions"

echo -e "${YELLOW}Important reminders:${NC}"
echo "- Never commit terraform.tfstate files"
echo "- Use GitHub Secrets for sensitive data in CI/CD"
echo "- Test changes locally before pushing"
echo "- Monitor connector logs in Fivetran Dashboard" 
