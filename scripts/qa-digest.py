#!/usr/bin/env python3
"""qa-digest.py — every 3 days, one comment Ori can read in a minute: what merged, and who approved it
(owner-chat-approve SPEC AC-10, AC-11; run by .github/workflows/qa-digest.yml, or by hand with --dry-run).

For each PR merged into the default branch since the previous digest it asks the merged-PR audit
(scripts/check-merged-pr-qa.py `audit()`, the same reading the per-merge check posts — never a second model of what a
valid approval is) and writes one line:
  QA walk (who walked)   — the QA account approved through qa-review.sh (a QA agent's, QA runner's or audited builder walk)
  (CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism or labels its record; claims no approval)
  Ori in chat            — approved in a session's chat; the line quotes what the walk did not test
  Ori override (GitHub)  — an OWNER OVERRIDE review posted in GitHub
  ⚠️ <why>               — the audit's alarm (no approval it can account for) or a read it could not make
PRs that touch nothing the QA rule covers (docs, tooling) are counted, not listed.

  python3 scripts/qa-digest.py --repo OWNER/NAME [--since 2026-10-18T00:00:00Z] [--dry-run]

Each digest covers [since, until): `until` is the moment it started, written into the comment as a hidden marker
(`<!-- qa-digest-until: … -->`), and the next digest starts exactly there — a PR merged while a run is working lands in
the next one, never in neither (fresh CTO review of #5278, MED-4). Without --since it starts at the newest marker on the
`qa-digest` issue (or 3 days back). It opens and pins that issue the first time. Exit 0 posted (or printed) · 1 the
digest itself could not be made.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
LABEL = "qa-digest"
TITLE = "QA digest — every merged PR and who approved it"
_UNTESTED_RE = re.compile(r"\*\*Not tested:\*\*\s*(.+)")
_UNTIL_RE = re.compile(r"<!-- qa-digest-until: (\S+) -->")
# Only the workflow's own comments move the window: a marker pasted by anyone else (a future date) would silently empty
# every later digest (QA charter of #5278, FW-9). A digest posted by hand is ignored too — its PRs are listed again.
DIGEST_AUTHORS = ("github-actions", "github-actions[bot]")
# Who GitHub notifies of each digest: the issue is opened by github-actions, so nobody watches it unless named (FW-5).
NOTIFY = "@Oriel00"
WALKERS = {"qa-agent": "a QA agent", "qa-runner": "the QA runner", "builder": "the builder's walk, audited"}
# The merged-PR audit judges a PR by the branch's rules NOW; before main's GitHub queue went live (native-merge-queue,
# 2026-10-17 ~01:30Z) merges went through the old merge line with its own record (labels), which the audit reads as
# "no approval" — 23 false alarms in a dry run of 2026-10-15..16. So no window starts earlier than this.
QUEUE_ERA_START = datetime(2026, 10, 17, 1, 30, tzinfo=timezone.utc)
LIST_LIMIT = 1000  # more than this in one window fails loudly instead of being cut silently (MED-4)


def _load_audit():
    spec = importlib.util.spec_from_file_location("_check_merged_pr_qa_for_digest", HERE / "check-merged-pr-qa.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


AUDIT = _load_audit()


def _gh_json(gh_call, args: list[str], what: str):
    rc, out, err = gh_call(args)
    if rc != 0:
        raise RuntimeError(f"GitHub could not be asked {what} ({(err or out).strip()[:200]})")
    try:
        return json.loads(out or "null")
    except ValueError as e:
        raise RuntimeError(f"GitHub's answer {what} could not be read ({e})") from None


def merged_between(gh_call, repo: str, base: str, since: datetime, until: datetime) -> list[dict]:
    stamp = since.strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = _gh_json(gh_call, ["pr", "list", "-R", repo, "--state", "merged", "--base", base, "--limit", str(LIST_LIMIT),
                              "--search", f"merged:>={stamp}", "--json",
                              "number,title,mergedAt,headRefOid,mergeCommit"], "for the merged PRs")
    if len(rows or []) >= LIST_LIMIT:
        raise RuntimeError(f"{LIST_LIMIT}+ PRs merged since {stamp} — too many for one digest; run it with a later "
                           f"--since, or raise LIST_LIMIT")
    out = []
    for r in rows or []:
        try:
            at = datetime.fromisoformat(str(r.get("mergedAt")).replace("Z", "+00:00"))
        except ValueError:
            continue
        if since <= at < until:
            out.append(r)
    return sorted(out, key=lambda r: r["mergedAt"])


def _plain(text: str) -> str:
    """A PR title or the agent's untested text, safe inside the comment: a stray `<!--` must not hide the lines after."""
    return (text or "").replace("<", "&lt;").replace(">", "&gt;").replace("\n", " ").replace("@", "@\u200b")


def untested_of(gh_call, repo: str, number: int) -> str:
    reviews = _gh_json(gh_call, ["api", f"repos/{repo}/pulls/{number}/reviews?per_page=100"], f"for #{number}'s reviews")
    for r in reversed(reviews or []):
        mark = AUDIT.QR.parse_marker(r.get("body"))
        if mark and mark.get("owner") == "chat":
            m = _UNTESTED_RE.search(r.get("body") or "")
            return m.group(1).strip() if m else "(not given)"
    return "(not found)"


def line_for(gh_call, repo: str, base: str, default: str, pr: dict) -> tuple[str, str | None]:
    """(kind, markdown line or None when nothing was owed)."""
    env = {"GITHUB_REPOSITORY": repo, "PR_NUMBER": str(pr["number"]), "BASE_REF": base, "DEFAULT_BRANCH": default,
           "HEAD_SHA": pr.get("headRefOid") or "", "MERGE_SHA": (pr.get("mergeCommit") or {}).get("oid") or ""}
    res = AUDIT.audit(env, gh_call, sleep=lambda _s: None)
    v, reason = res.get("verdict"), res.get("reason", "")
    head = f"#{pr['number']} {_plain(pr.get('title', ''))[:80]}"
    if v == AUDIT.NOTHING:
        return "none", None
    if v == AUDIT.QA_VERDICT:
        by = WALKERS.get(res.get("walked_by") or "", "walker not recorded")
        return "qa", f"- ✅ **QA walk** ({by}) — {head}"
    if v == AUDIT.OVERRIDE and res.get("owner_kind") == "chat":
        return "chat", f"- 💬 **Ori in chat** — {head} · not tested: {_plain(untested_of(gh_call, repo, pr['number']))}"
    if v == AUDIT.OVERRIDE:
        return "github", f"- 📱 **Ori override (GitHub)** — {head}"
    return "alarm", f"- ⚠️ **{v}** — {head} · {_plain(reason)[:240]}"


def digest(gh_call, repo: str, base: str, default: str, since: datetime, now: datetime) -> str:
    floor_note = ""
    if since < QUEUE_ERA_START:
        since = QUEUE_ERA_START
        floor_note = (f"\n_Starts at {QUEUE_ERA_START:%Y-%m-%d %H:%M} UTC, when main moved to GitHub's merge queue — "
                      f"earlier merges are on the old merge line's own record._\n")
    counts = {"qa": 0, "chat": 0, "github": 0, "alarm": 0, "none": 0}
    lines = []
    for pr in merged_between(gh_call, repo, base, since, now):
        kind, line = line_for(gh_call, repo, base, default, pr)
        counts[kind] += 1
        if line:
            lines.append(line)
    app = counts["qa"] + counts["chat"] + counts["github"] + counts["alarm"]
    head = (f"## QA digest — {since:%Y-%m-%d %H:%M} → {now:%Y-%m-%d %H:%M} UTC\n\n"
            # CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism or labels its record; claims no approval
            f"**{app} app PR(s) merged:** {counts['qa']} by a QA walk · {counts['chat']} approved by Ori in chat · "
            f"{counts['github']} by Ori's GitHub override · {counts['alarm']} ⚠️ alarm(s). "
            f"{counts['none']} other PR(s) needed no QA (docs / tooling).\n")
    head += floor_note
    if counts["alarm"]:
        head += "\n**⚠️ Needs a look:** each alarm also has its own `qa-audit-alert` issue.\n"
    body = head + ("\n" + "\n".join(lines) + "\n" if lines else "\nNo app PR merged in this window.\n")
    return body + f"\ncc {NOTIFY}\n\n<!-- qa-digest-until: {now.strftime('%Y-%m-%dT%H:%M:%S.%fZ')} -->\n"


ISSUE_QUERY = ("query($o:String!,$n:String!){repository(owner:$o,name:$n){issues(labels:[\"" + LABEL + "\"],"
               "states:OPEN,first:5,orderBy:{field:CREATED_AT,direction:ASC}){nodes{number "
               "comments(last:30){nodes{body author{login}}}}}}}")


def digest_issue(gh_call, repo: str) -> tuple[int | None, datetime | None]:
    """The oldest open qa-digest issue and the newest `until` marker among its LAST 30 comments (the newest, not the
    first 100 `gh issue list` returns — fresh CTO review of #5278, MED-5) written by the workflow (FW-9)."""
    owner, name = repo.split("/", 1)
    data = _gh_json(gh_call, ["api", "graphql", "-f", f"query={ISSUE_QUERY}", "-F", f"o={owner}", "-F", f"n={name}"],
                    "for the qa-digest issue")
    nodes = (((data or {}).get("data") or {}).get("repository") or {}).get("issues", {}).get("nodes") or []
    if not nodes:
        return None, None
    issue = nodes[0]
    stamps = [m.group(1) for c in (issue.get("comments") or {}).get("nodes") or []
              if ((c.get("author") or {}).get("login") or "") in DIGEST_AUTHORS
              for m in [_UNTIL_RE.search(c.get("body") or "")] if m]
    last = max(stamps) if stamps else None
    return issue["number"], (datetime.fromisoformat(last.replace("Z", "+00:00")) if last else None)


def main(argv: list[str] | None = None, gh_call=None, now: datetime | None = None) -> int:
    ap = argparse.ArgumentParser(prog="qa-digest", description=__doc__.split("\n\n")[0])
    ap.add_argument("--repo", required=True)
    ap.add_argument("--base", default="main")
    ap.add_argument("--since", help="ISO time (UTC) to start from; default: after the previous digest, or 3 days back")
    ap.add_argument("--dry-run", action="store_true", help="print the digest; post nothing")
    a = ap.parse_args(argv)
    gh_call = gh_call or AUDIT.gh
    now = now or datetime.now(timezone.utc)
    try:
        issue, last = digest_issue(gh_call, a.repo)
        since = (datetime.fromisoformat(a.since.replace("Z", "+00:00")) if a.since else
                 last or now - timedelta(days=3))
        # until = 10 min back, whole seconds: GitHub's search index is eventually consistent, so a PR merged in the
        # last minutes may not be listed yet — it belongs to the next digest (fresh CTO review of #5278, round 2 LOW-1)
        until = now.replace(microsecond=0) - timedelta(minutes=10)
        body = digest(gh_call, a.repo, a.base, a.base, since, until)
        if a.dry_run:
            print(body)
            return 0
        if issue is None:
            gh_call(["label", "create", LABEL, "-R", a.repo, "--force", "--color", "0E8A16",
                     "--description", "The 3-day QA digest (owner-chat-approve)"])
            created = _gh_json(gh_call, ["api", f"repos/{a.repo}/issues", "-f", f"title={TITLE}", "-f",
                                         "body=Every 3 days: each PR merged into main and who approved it "
                                         "(owner-chat-approve). Posted by .github/workflows/qa-digest.yml.",
                                         "-f", f"labels[]={LABEL}"], "to open the qa-digest issue")
            issue = created["number"]
            gh_call(["issue", "pin", str(issue), "-R", a.repo])  # a failed pin is cosmetic; the digest still posts
        rc, out, err = gh_call(["issue", "comment", str(issue), "-R", a.repo, "--body", body])
        if rc != 0:
            raise RuntimeError(f"the digest could not be posted on #{issue} ({(err or out).strip()[:200]})")
        print(f"posted the QA digest on #{issue}\n\n{body}")
        return 0
    except (RuntimeError, KeyError, TypeError) as e:
        print(f"qa-digest: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
