# Fivetran State Manager: Architecture

```mermaid
flowchart TB
    Clients["Clients<br/>(HTTP, RPC, Fivetran, etc.)"]
    
    subgraph Server["App Servers"]
        FastAPI["FastAPI Endpoints<br/>/state/get<br/>/state/update<br/>/state/get-multiple<br/>/state/update-multiple"]
        subgraph DBOS["DBOS Library"]
            Workflows["Durable Workflows<br/>get_state_workflow<br/>update_state_workflow<br/>get_state_multiple_workflow<br/>update_state_multiple_workflow"]
            Steps["DBOS Steps<br/>get_connection_state_step<br/>pause_connection_step<br/>update_state_step<br/>resume_connection_step<br/>verify_state_step"]
        end
    end
    
    FivetranAPI["Fivetran API<br/>api.fivetran.com"]
    Database[("Database<br/>DBOS State Management")]
    
    Clients <-->|HTTP Requests| FastAPI
    FastAPI -->|Orchestrates| Workflows
    Workflows -->|Executes| Steps
    Steps <-->|API Calls| FivetranAPI
    DBOS <-->|State Persistence| Database
    
    style Server fill:#d3d3d3,stroke:#ff8c00,stroke-width:3px
    style Database fill:#d3d3d3,stroke:#ff8c00,stroke-width:3px
    style FastAPI fill:#ffffff
    style Workflows fill:#ffffff
    style Steps fill:#ffffff
    style Clients fill:#e1f5e1
    style FivetranAPI fill:#e1f5e1
```

## Architecture Overview

This diagram illustrates the architecture of the Fivetran Connection State Manager:

1. **Clients**: External systems making HTTP requests (including Fivetran itself)
2. **App Servers**: FastAPI application with REST endpoints for state management
3. **DBOS Library**: 
   - **Durable Workflows**: Orchestrate multi-step operations with automatic recovery
   - **DBOS Steps**: Individual atomic operations that interact with Fivetran API
4. **Fivetran API**: External service for managing connection state
5. **Database**: DBOS-managed database for workflow state persistence and durability

The architecture ensures reliable state management through DBOS's durable execution model, where workflows can recover from failures and maintain consistency.
