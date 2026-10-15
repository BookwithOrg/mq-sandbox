#!/usr/bin/env python3
"""_queue_live.py — does GitHub require a merge queue on this branch, and which checks does it require?

ONE place that asks, because several tools must agree on the answer (the queue command today; the
landed-equals-tested check, the merge line's refusal and the fragment readers later — SPEC AC4.1, AC2.9,
AC5.2). "Queue live" is never a flag we keep: it is GitHub's own answer, read from the rules that apply
to the branch (`GET /repos/{owner}/{repo}/rules/branches/{branch}` — every ruleset layered on that branch,
already merged). A caller that cannot get an answer gets "unknown", never a guess: the callers decide what
an unread answer means for them (the queue command refuses; a check that cannot ask must not report green).

`gh` is passed in — a callable taking the argument list and returning (exit code, stdout, stderr) — so
this is testable without a network (scripts/__tests__/test_queue_pr.py).
"""

from __future__ import annotations

import json
from urllib.parse import quote


def queue_state(repo: str, branch: str, gh) -> "tuple[str, list[str] | str]":
    """("live", required check names) · ("none", required check names) · ("unknown", why)."""
    # A branch name can hold `/` (mq-rehearsal/2026-10-09): encode it so it stays ONE path segment.
    # per_page=100: the default page is 30 rules, and a merge-queue rule on page 2 would read as "no queue".
    rc, out, err = gh(["api", f"repos/{repo}/rules/branches/{quote(branch, safe='')}?per_page=100"])
    if rc != 0:
        return "unknown", (err or out or f"gh exited {rc}").strip()[:300]
    try:
        rules = json.loads(out)
    except ValueError:
        return "unknown", "GitHub's answer was not JSON"
    if not isinstance(rules, list) or not all(isinstance(r, dict) for r in rules):
        return "unknown", "GitHub's answer was not a list of rules"
    required: "list[str]" = []
    for rule in rules:
        if rule.get("type") == "required_status_checks":
            for check in (rule.get("parameters") or {}).get("required_status_checks") or []:
                name = check.get("context") if isinstance(check, dict) else None
                if name and name not in required:
                    required.append(name)
    live = any(rule.get("type") == "merge_queue" for rule in rules)
    return ("live" if live else "none"), required
