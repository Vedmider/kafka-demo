# Hard enforcement (opt-in) — Claude Code

The policy in [`../../AGENTS.md`](../../AGENTS.md) is advisory. If you observe an agent grepping
around this small repo instead of reading the (few, well-named) source files, this adds a speed bump.

## What it does

Registers a Claude Code **`PreToolUse`** hook on native **`Grep`**/**`Glob`** that returns
`permissionDecision: "ask"` — Claude pauses and asks you to confirm, nudging it to read the source
directly. It **asks, never blocks**; a genuine text/log/config search proceeds on confirm. It touches
only `Grep`/`Glob`, reads nothing, sends nothing. Claude-specific (hook schemas aren't portable);
other clients still get the soft policy via `AGENTS.md`.

## Install

```bash
mkdir -p .claude
cp hooks/claude/settings.json .claude/settings.json
```

If you already have `.claude/settings.json`, merge the `hooks.PreToolUse` array by hand.

## Remove

Delete the `PreToolUse` entry (or `.claude/settings.json` if that's all it held). The script is inert
unless referenced by settings.
