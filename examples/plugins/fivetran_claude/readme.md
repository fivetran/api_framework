---
name: fivetran-skills
description: Fivetran plugin with comprehensive API coverage, intelligent workflows, and AI automation for rapid data pipeline deployment.
metadata:
  short-description: Fivetran Skills Plugin for AI Automation
  team: FDE
  owner: "Elijah Davis <elijah.davis@fivetran.com>"
  version: "2.0.0"
  api_coverage: "all endpoints"
---

# Fivetran Claude Plugin v2.0

**Fivetran automation with comprehensive API coverage and AI-powered workflows.**

This plugin transforms Claude into a powerful Fivetran automation engine, covering all 180+ API endpoints with intelligent workflows, multi-step automation, and enterprise-ready reliability. Perfect for rapid data pipeline deployment and management at scale.

## Key Features

- **AI-Powered Workflows**: Intelligent automation for complex multi-step operations
- **Complete API Coverage**: All 180+ Fivetran REST API endpoints supported
- **Enterprise Ready**: Production-grade reliability with comprehensive error handling
- **Multi-Step Automation**: End-to-end pipeline creation with single commands
- **Intelligent Monitoring**: Automated health checks and performance optimization
- **Token Conscious**: Optimized for efficient Claude token usage

## Quick Start

### Authentication (one-time setup)

This skill reads API credentials from a config file so you never need to paste secrets
into the chat. Fill in your credentials at:

```
<workspace>/.fivetran/credentials.json
```

The file should look like this:

```json
{
  "api_key": "YOUR_FIVETRAN_API_KEY",
  "api_secret": "YOUR_FIVETRAN_API_SECRET"
}
```

You can find your API key and secret in the Fivetran dashboard under **Settings → API Config**.

> **Note for Claude:** The scripts auto-load credentials from `.fivetran/credentials.json`
> in the plugin folder — no manual setup needed. If a script reports missing credentials,
> ask the user to fill in `.fivetran/credentials.json` rather than pasting secrets into the chat.
   ```

2. Install dependencies:
   ```bash
   pip install requests colorama
   ```

## Multi-Step Workflow Examples

### Complete Data Pipeline Creation
```
"Create my complete data pipeline: MySQL at mysql.company.com to Snowflake account company.snowflake, include dbt transformations, start syncing, and monitor health."
```

### Intelligent Troubleshooting
```
"My Salesforce connector is failing. Analyze the issue, provide root cause analysis, and implement fixes automatically."
```

### Enterprise Migration
```
"Migrate all connectors from group 'dev_123' to 'prod_456' with zero downtime and full validation."
```

## Core Skills Architecture

### 1. **Enterprise API Wrapper** (`fivetran_api.py`)
- Complete coverage of all Fivetran REST API endpoints
- Intelligent retry logic with exponential backoff
- Comprehensive error handling and logging
- Session management with connection pooling

**Key Endpoints Covered:**
- **Connections**: Create, read, update, delete, sync, test
- **Destinations**: Full lifecycle management with optimization
- **Transformations**: dbt project management and orchestration
- **Groups & Users**: Team and access management
- **Webhooks**: Event-driven automation setup
- **Metadata**: Intelligent service discovery

### 2. **Intelligent Connector Management** (`connector_skill.py`)
**Sample Prompts:**
- *"Create my MySQL connector with intelligent configuration detection"*
- *"Start my Salesforce connector and monitor until first sync completes"*
- *"Discover requirements for Google Analytics connector setup"*

**Capabilities:**
- Intelligent configuration building based on service metadata
- Automated service discovery with setup guidance
- Real-time sync monitoring with health assessment
- Performance optimization recommendations

### 3. **Advanced Destination Management** (`destination_skill.py`)
**Sample Prompts:**
- *"Create my Snowflake destination with enterprise security settings"*
- *"Test all destinations in production group and provide health recommendations"*
- *"Set up multi-cloud destinations for disaster recovery"*

**Capabilities:**
- Enterprise-grade destination setup with best practices
- Automated connectivity testing and validation
- Security considerations and recommendations
- Performance monitoring and optimization

### 4. **Transformation Orchestration** (`transformation_skill.py`)
**Sample Prompts:**
- *"Create my dbt transformation project with staging and mart layers"*
- *"Set up Git integration for my transformation with CI/CD pipeline"*
- *"Generate dbt project template for e-commerce analytics"*

**Capabilities:**
- Complete dbt project lifecycle management
- Git repository integration with branch strategies
- Automated testing and validation workflows
- Best practices implementation for data modeling

### 5. **AI Agent Orchestration** (`ai_agent_skill.py`)
**Sample Prompts:**
- *"Execute full pipeline workflow from MySQL to Snowflake with transformations"*
- *"Troubleshoot sync failures across all my connectors"*
- *"Optimize performance for high-volume data pipelines"*

**Advanced Workflows:**
- **Full Pipeline**: End-to-end pipeline creation and deployment
- **Connector Migration**: Zero-downtime migration with validation
- **Data Validation**: Comprehensive quality assurance across pipelines  
- **Performance Optimization**: Automated tuning and scaling

## Comprehensive Capabilities

### Connection Management
- List, create, update, delete connections
- Intelligent configuration with service-specific optimizations
- Real-time sync monitoring and health assessment
- Automated troubleshooting and issue resolution

### Destination Management  
- Multi-cloud destination setup (Snowflake, BigQuery, Redshift, etc.)
- Enterprise security configuration and best practices
- Performance monitoring and optimization recommendations
- Disaster recovery and backup strategies

### Transformation Management
- Complete dbt project lifecycle management
- Git integration with automated CI/CD
- Data quality testing and validation
- Performance monitoring and optimization

### Enterprise Features
- **Multi-tenant support**: Group and user management
- **Webhook automation**: Event-driven workflows
- **Comprehensive monitoring**: Health checks and alerting
- **Security**: Best practices implementation
- **Scalability**: High-volume data pipeline support

## Sample Multi-Step Workflows

### Enterprise Data Platform Setup
```
"Set up enterprise data platform: create groups for Marketing, Sales, Finance teams. Each needs Snowflake destination, team-specific connectors, and shared dbt transformations."
```

### Disaster Recovery Implementation  
```
"Create disaster recovery setup: replicate production connectors to backup region, set up monitoring, and establish failover procedures."
```

### Performance Optimization Campaign
```
"Analyze and optimize performance across all my data pipelines: identify bottlenecks, apply optimizations, and establish monitoring."
```

## Enterprise Use Cases

- **Rapid Pipeline Deployment**: Complete data pipelines in minutes
- **Multi-Team Data Platforms**: Scalable infrastructure for large organizations  
- **Compliance & Governance**: Automated data quality and lineage tracking
- **Cost Optimization**: Intelligent resource management and scaling
- **Disaster Recovery**: Automated backup and failover procedures

## Documentation

- **[Sample Prompts](SAMPLE_PROMPTS.md)**: Comprehensive examples for all use cases
- **API Reference**: Complete endpoint documentation with examples
- **Best Practices**: Enterprise deployment and security guidelines
- **Troubleshooting**: Common issues and automated solutions

## Legacy Capabilities (Maintained)

### 1. Management (CRUD & Discovery)
- **Discover Requirements**: `python scripts/manage.py discover <service>`
- **List Connectors**: `python scripts/manage.py list`
- **Check Status**: `python scripts/manage.py status <connector_id>`
- **Create Connector**: `python scripts/manage.py create --service <service> --group <group_id> --config <config.json>`

### 2. Health & Monitoring
- **Health Check**: `python scripts/health_report.py check <connector_id>`
- **Global Audit**: `python scripts/health_report.py audit [--group <group_id>]`
- **Metrics**: `python scripts/health_report.py metrics <connector_id>`

### 3. Migration
- **Migrate**: `python scripts/migrate.py <source_connector_id> <target_group_id>`
- **Migrate & Sync**: `python scripts/migrate.py <source_connector_id> <target_group_id> --sync`
