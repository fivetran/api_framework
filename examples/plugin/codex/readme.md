# Fivetran Codex Plugin: Setup, Configuration, and Prompts

This guide covers everything from git clone to first successful workflow run in Codex.

## Setup Checklist

- Codex app installed
- Fivetran API key + secret
- Python 3.9+

## 1) Clone and prepare runtime

```bash
git clone <YOUR_REPO_URL>
cd <repo>/plugin/codex
python3 -m venv .venv
source .venv/bin/activate
pip install requests colorama
```

## 2) Configure credentials

Update `.fivetran/credentials.json`:

```json
{
  "api_key": "YOUR_FIVETRAN_API_KEY",
  "api_secret": "YOUR_FIVETRAN_API_SECRET"
}
```

## 3) Register the plugin for Codex

Copy plugin to the local plugin path:

```bash
mkdir -p ~/plugins
rsync -a --delete ./ ~/plugins/codex_plugin_hackathon/
```

Create or update `~/.agents/plugins/marketplace.json` and include this plugin entry:

```json
{
  "name": "openai-local",
  "interface": {
    "displayName": "Local Plugins"
  },
  "plugins": [
    {
      "name": "codex_plugin_hackathon",
      "source": {
        "source": "local",
        "path": "./plugins/codex_plugin_hackathon"
      },
      "policy": {
        "installation": "AVAILABLE",
        "authentication": "ON_INSTALL"
      },
      "category": "Developer Tools"
    }
  ]
}
```

Restart Codex after updating marketplace metadata.

## 4) Smoke test

From the plugin root, run:

```bash
python scripts/manage.py list
python scripts/manage.py discover salesforce
```

If both commands return JSON output, setup is complete.

## Prompt Examples

### Connector creation

- `Create a Salesforce connector in group <group_id> and start an initial sync.`
- `Discover required fields for mysql and show me a config template.`

### Monitoring and troubleshooting

- `Audit all connectors in group <group_id> and rank issues by severity.`
- `Check connector <connector_id> and explain why the last sync failed.`

### Destination operations

- `Create a Snowflake destination for group <group_id> with warehouse XSMALL.`
- `Run setup tests on destination <destination_id> and summarize failures.`

### Schema and sync orchestration

- `Compare schema drift between connectors <source_id> and <target_id>.`
- `Run sync chain in order: <id_1> -> <id_2> -> <id_3>, stop on first failure.`

### Transformations

- `List transformations for group <group_id> and run transformation <transformation_id>.`
- `Run full refresh for transformation <transformation_id>.`

## Notes for extending this plugin

- Keep skill frontmatter `description` highly explicit so Codex can match intent correctly.
- Keep scripts at the plugin root and call them from that root.
- Do not commit `.fivetran/credentials.json`.
