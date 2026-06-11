#!/usr/bin/env node

import { execFile } from "node:child_process";
import { promisify } from "node:util";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

const execFileAsync = promisify(execFile);
const localTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;

async function runAppleScript(script) {
  const { stdout } = await execFileAsync("/usr/bin/osascript", ["-e", script]);
  return stdout.trim();
}

function parseLocalDateTime(value) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("Datetime value must be a non-empty string.");
  }

  if (/[zZ]|[+-]\d{2}:\d{2}$/.test(value)) {
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) {
      throw new Error(
        `Unsupported datetime "${value}". Use ISO-8601 such as 2026-06-09T14:00:00 or 2026-06-09T14:00:00-06:00.`
      );
    }
    return dt;
  }

  const dt = new Date(`${value}`);
  if (Number.isNaN(dt.getTime())) {
    throw new Error(
      `Unsupported datetime "${value}". Use ISO-8601 such as 2026-06-09T14:00:00 or 2026-06-09T14:00:00-06:00.`
    );
  }
  return dt;
}

function toAppleDateString(date) {
  return date.toLocaleString("en-US", {
    weekday: "long",
    month: "long",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: true,
    timeZone: localTimeZone,
  }).replace(",", "");
}

function escapeAppleScript(value) {
  return String(value).replaceAll("\\", "\\\\").replaceAll('"', '\\"');
}

async function getCalendarNames() {
  const output = await runAppleScript('tell application "Calendar" to get name of every calendar');
  return output
    .split(",")
    .map((name) => name.trim())
    .filter(Boolean);
}

async function validateCalendarName(calendar) {
  const calendars = await getCalendarNames();
  if (calendars.includes(calendar)) {
    return null;
  }

  const caseInsensitive = calendars.find((name) => name.toLowerCase() === calendar.toLowerCase());
  if (caseInsensitive) {
    return `Calendar "${calendar}" was not found exactly. Use the exact calendar name "${caseInsensitive}".`;
  }

  return `Calendar "${calendar}" was not found. Available calendars: ${calendars.join(", ") || "none"}.`;
}

function textResult(text, isError = false) {
  return {
    content: [{ type: "text", text }],
    isError,
  };
}

const server = new McpServer({
  name: "my_google_calendar",
  version: "0.1.0",
});

server.tool(
  "list_calendars",
  "List local macOS Calendar calendars. Use this first to discover the exact calendar name for event creation.",
  {},
  async () => {
    const calendars = await getCalendarNames();
    return textResult(JSON.stringify({ calendars }, null, 2));
  }
);

server.tool(
  "block_time",
  "Block time on a validated local macOS Calendar calendar. Use this after availability has been confirmed. Local times without an offset are interpreted in the Mac's local timezone.",
  {
    calendar: z.string().describe("Exact calendar name as shown in Calendar.app."),
    title: z.string().describe("Event title."),
    start_local: z
      .string()
      .describe("Local start datetime in ISO-8601. Examples: 2026-06-09T14:00:00 or 2026-06-09T14:00:00-06:00."),
    end_local: z
      .string()
      .describe("Local end datetime in ISO-8601. Examples: 2026-06-09T14:30:00 or 2026-06-09T14:30:00-06:00."),
    notes: z.string().optional().describe("Optional event notes."),
    location: z.string().optional().describe("Optional event location."),
  },
  async ({ calendar, title, start_local, end_local, notes = "", location = "" }) => {
    const calendarError = await validateCalendarName(calendar);
    if (calendarError) {
      return textResult(calendarError, true);
    }

    const start = parseLocalDateTime(start_local);
    const end = parseLocalDateTime(end_local);
    if (end <= start) {
      return textResult("end_local must be after start_local.", true);
    }

    const startApple = toAppleDateString(start);
    const endApple = toAppleDateString(end);

    const script = `
tell application "Calendar"
  tell calendar "${escapeAppleScript(calendar)}"
    set newEvent to make new event with properties {summary:"${escapeAppleScript(title)}", start date:(date "${escapeAppleScript(startApple)}"), end date:(date "${escapeAppleScript(endApple)}"), description:"${escapeAppleScript(notes)}", location:"${escapeAppleScript(location)}"}
    return id of newEvent
  end tell
end tell
`.trim();

    const eventId = await runAppleScript(script);
    return textResult(
      JSON.stringify(
        {
          status: "blocked",
          calendar,
          title,
          start_local: start.toISOString(),
          end_local: end.toISOString(),
          event_id: eventId,
          local_timezone: localTimeZone,
        },
        null,
        2
      )
    );
  }
);

const transport = new StdioServerTransport();
await server.connect(transport);
