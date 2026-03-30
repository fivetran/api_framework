# Fivetran Connector SDK CI/CD with Terraform

This is an Infrastructure as Code (IaC) framework for deploying Fivetran custom connectors using the Connector SDK and Terraform. 

## IaC with Connector SDK

Integrating Fivetran's Connector SDK with Terraform unlocks scalability and reliability for custom data pipelines:
- **Automated Deployments:** Eliminate manual UI configurations and ensure repeatable, error-free connector deployments.
- **Version-Controlled Infrastructure:** Treat data pipelines as code. Audit, review, and rollback connector configurations alongside your application code.
- **Unified CI/CD Integration:** Automatically validate schema changes, test integration logic locally, and provision resources securely via automated pipelines.
- **Multi-Environment Consistency:** Standardize deployments across Development, Staging, and Production environments using parameterized variable injection.

## Overview

```text
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   GitHub Repo   │───▶│  GitHub Actions  │───▶│   Terraform     │
│                 │    │   CI/CD Pipeline │    │   Infrastructure│
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                              │
         │                                              ▼
         │                                    ┌─────────────────┐
         │                                    │   Fivetran      │
         │                                    │   Connector     │
         └────────────────────────────────────▶│   SDK Service   │
                                              └─────────────────┘
```

## Setup & Deployment Guide

Follow these instructions to safely validate, test, and deploy your custom SDK connector infrastructure.

### Step 1: Install Prerequisites

Ensure you have the following enterprise tooling installed on your local machine or CI runner:
- **Terraform** (v1.5.0+)
- **Python** (3.9+)
- **Fivetran CLI**
- **jq** (for JSON parsing)

### Step 2: Configure Environment Variables

Export the required Fivetran and Destination credentials to your local environment. Replace the placeholders (`<...>`) with your actual configuration details. 

*Note: For CI/CD, these should be configured as GitHub Repository Secrets.*

```bash
# Fivetran API Configuration
export FIVETRAN_API_KEY="<YOUR_FIVETRAN_API_KEY_BASE64>"
export FIVETRAN_API_SECRET="<YOUR_FIVETRAN_API_SECRET>"
export FIVETRAN_DESTINATION="<YOUR_DESTINATION_NAME>"
export FIVETRAN_CONNECTION="<YOUR_CONNECTION_NAME>"

# Target CSDK Configuration Placeholders
export ACCOUNT="<YOUR_ACCOUNT>"
export USER="<YOUR_USER>"
export SECRET_KEY="<YOUR_SECRET_KEY>"
```

### Step 3: Validate Environment Setup

Run the setup validation script. This ensures all required tools, scripts, and configuration files (`configuration.json`, `terraform.tfvars`) are present and correctly formatted.

```bash
chmod +x test-setup.sh
./test-setup.sh
```
*Expected Output: `Setup validation completed!`*

### Step 4: Local Connector Testing

Before deploying infrastructure, validate the connector's python logic locally. This step provisions isolated python dependencies and executes a dry-run of the SDK schema derivation and update logic.

```bash
chmod +x deploy.sh
./deploy.sh test
```
*Expected Output: `Local connector test completed`*

### Step 5: Plan Infrastructure Changes

Preview the Terraform changes that will be applied to your Fivetran workspace. This is a critical step for auditing infrastructure modifications before they occur.

```bash
./deploy.sh plan
```
*Review the output carefully to ensure the intended groups, destinations, and connections will be created or modified.*

### Step 6: Execute Deployment

Deploy the connector payload to Fivetran's servers and execute the Terraform apply to formally provision the surrounding configurations.

```bash
./deploy.sh deploy
```
*Expected Output: `Terraform deployment completed successfully!`*

### Step 7: Verify Status

Check the current state of your Fivetran deployment and Terraform resources to ensure successful synchronization.

```bash
./deploy.sh status
```

---

## Best Practices

- **Never Commit Secrets:** Do not hardcode actual API keys in `terraform.tfvars`. Use environment variables or a secure secrets manager like AWS Secrets Manager or HashiCorp Vault.
- **CI/CD Secrets:** When deploying via GitHub Actions, ensure all credential placeholders are securely mapped to GitHub Repository Secrets.
- **Least Privilege:** Provision destination users (e.g., service accounts) and Fivetran API Keys with the absolute minimum permissions necessary to execute the pipeline.

## Troubleshooting

- **Check Connection Status:** Execute `./deploy.sh fivetran-status` to ping the Fivetran API for real-time connection telemetry.
- **State Backups:** Use `./deploy.sh backup` prior to major changes. Terraform state is fragile; always back it up before manual interventions.
- **Documentation:** Refer to the [Fivetran Connector SDK Documentation](https://fivetran.com/docs/connector-sdk) for detailed SDK capabilities and limitations.
