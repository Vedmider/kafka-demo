#!/usr/bin/env bash
# Claude Code PreToolUse hook (opt-in). Fires before a native Grep/Glob.
# This is a small, well-structured learning repo — reading the source (common/producer/consumer)
# is usually faster than grepping. It ASKS, it does not block; confirm and proceed for a genuine
# text/log/config search. See https://docs.anthropic.com/en/docs/claude-code/hooks for the contract.

cat <<'JSON'
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "ask",
    "permissionDecisionReason": "kafka-demo is a small, well-structured repo (common/producer/consumer). Reading the relevant source directly is usually clearer than grep. Confirm if you really mean a literal text/log/config search. See AGENTS.md."
  }
}
JSON
exit 0
