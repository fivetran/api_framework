# Local Calendar Stdio MCP

Minimal local MCP server for Codex CLI that creates events through macOS `Calendar.app`.

## What it does

- `list_calendars`: returns the exact local calendar names
- `block_time`: blocks time in a validated named calendar

If your Google account is synced in `Calendar.app`, created events will sync to Google Calendar.

## Setup

Install dependencies in the server directory:

```bash
npm install
```

## Codex config

Add `my_google_calendar` in `~/.codex/config.toml` with:

```toml
[mcp_servers.my_google_calendar]
command = "/usr/local/bin/node"
args = ["file_path_to/server.mjs"]
enabled = true
```

Then restart Codex CLI.

## macOS permission

The first time the server creates an event, macOS may prompt for automation permission for:

- `python3` controlling `Calendar`

Approve it in `System Settings > Privacy & Security > Automation` if prompted.

## Suggested prompts

```text
Use my_google_calendar list_calendars to show me the exact calendar names available on this Mac.
```

```text
Use my_google_calendar block_time to block 30 minutes on calendar "Calendar" from 2026-06-09T14:00:00 to 2026-06-09T14:30:00 titled "Codex CLI test".
```

## Notes

- This server does not check availability. Use the built-in Google Calendar connector for that.
- `block_time` validates the calendar name before writing.
- `block_time` normalizes local times. If you omit a timezone offset, the time is interpreted in the Mac's local timezone.
- This server is intentionally small and local-first. It avoids Google OAuth and remote MCP hosting.
