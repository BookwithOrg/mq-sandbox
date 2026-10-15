#!/usr/bin/env python3
"""queue-pr.py — put ONE exact commit of a pull request into GitHub's merge queue (SPEC AC4.1, AC4.6).

    python3 scripts/queue-pr.py --pr <N> [--sha <commit>] [--no-wait] [--repo <owner/name>]
    python3 scripts/queue-pr.py --status <N> [--repo <owner/name>]

WHAT IT IS. GitHub's merge queue tests a PR on top of the latest base branch and merges it by itself, on
GitHub's servers. This command is the door into it: it checks a few things GitHub does not, asks GitHub to
queue the PR pinned to the commit it just checked (`enqueuePullRequest(expectedHeadOid)`), says
"QUEUED, NOT MERGED", and exits. It does NOT wait for the merge — queued is not merged.

WHAT IT NEVER DOES. It never rebases, pushes, stamps, folds or edits the PR. Besides the queue request it
writes two things on GitHub, both visible on the PR: the label `queue-when-green` while it waits for the
PR's own checks (taken off on every way out), and a label plus a comment when the owner's schema-probe
escape was used. It does not judge QA: where the branch's rules require an approval, GitHub itself refuses
to queue a PR without one, and this command prints that refusal.

WHERE IT ACTS. Only on a PR whose base branch has a live merge queue — asked of GitHub's rules for that
branch (scripts/_queue_live.py), never assumed. Until the switch (SPEC AC5.2) `main` has none, so on a PR
into `main` it refuses and points to today's way of merging. "Allow auto-merge" stays off in this
repository: the queue request is direct, which is why the PR's required checks must be green first (GitHub
refuses a queue request while one is running) — hence the bounded wait.

THE SCHEMA PROBE. A PR that changes `shared/schema/**` must not land before prod has the columns it declares
(PR #755). The merge line already has that probe; this command calls the same decision (`prod_schema_decision`
in merge-driver.py, loaded, not copied), for the exact head, before anything is touched. The probe belongs
to the repository THIS SCRIPT lives in, and reads the commit from this script's own checkout — so it runs
for that repository's PRs from whatever folder the command is started in, and never for another repository.
Its owner-only escape is the merge line's: SKIP_PROD_SCHEMA_PROBE="#<pr>: <reason>".
"""

from __future__ import annotations

import argparse
import functools
import importlib.util
import json
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Callable

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from _queue_live import queue_state  # noqa: E402

# Exit codes. merge-driver.py owns 1–7 with OTHER meanings (3 = the gate was red, 7 = re-run to rejoin the
# line), so none of them is reused here: a caller that knows the merge line's table can never misread these.
# 8 is the merge line's own code for the same schema-probe refusal.
EXIT_PROBE = 8            # the prod-schema probe refused, or could not be run for this commit
EXIT_QUEUED = 10          # QUEUED, NOT MERGED — also when the PR was already in the queue
EXIT_NOT_GREEN_YET = 11   # a required check has not finished: --no-wait, or the wait ran out — try later
EXIT_CANNOT_ASK = 12      # GitHub could not be asked (network, login), or its answer to a SENT request was lost
EXIT_NO_QUEUE = 13        # the PR's base branch has no merge queue
EXIT_PR_STATE = 14        # the PR cannot be queued as it is: draft, closed, already merged, from a fork, in conflict
EXIT_HEAD = 15            # not the commit that was meant: --sha differs, the PR moved, or work is not pushed
EXIT_CHECKS = 16          # a required check is RED (or the branch requires none) — waiting will not help
EXIT_GITHUB_REFUSED = 17  # GitHub refused the queue request (its words are printed)
EXIT_USAGE = 64           # the command line could not be understood, or there is no such PR / repository
STATUS_MERGED = 0              # --status: merged, every commit landed
STATUS_NOT_MERGED = 20         # --status: not merged and not in the queue (dropped, closed, not queued yet)

WAIT_LABEL = "queue-when-green"
ESCAPE_LABEL = "prod-schema-probe-skipped"  # the merge line's label for the same escape
QUEUED_LINE = "QUEUED, NOT MERGED"
SETTLE_POLL_S = 5     # --status: a PR leaving the queue is asked again this often (it lands in about 1 s, row 28)
SETTLE_TRIES = 3      # ... this many times; the UNKNOWN answer's "after N s" is the two multiplied
READBACK_POLL_S = 5   # a lost queue reply: seconds between read-backs (GitHub's read side lagged 11 s, row 30)
READBACK_TRIES = 4    # ... the first read is at once, so the window is (TRIES - 1) x POLL = 15 s
EXIT_TABLE = f"""Until the switch to GitHub's queue, `main` has no queue: for a PR into main this command stops with exit
{EXIT_NO_QUEUE} and nothing is queued (a draft or conflicting PR stops earlier, with {EXIT_PR_STATE}). It waits for the
PR's own checks for as long as the merge line does (about 120 minutes at most); `--no-wait` makes it stop at once.

exit codes when queueing (--pr) — none of them is one of the merge line's 1–7:
  {EXIT_QUEUED}  {QUEUED_LINE} — GitHub lands it when its queue run passes. This is success; it is not 0
      so that no script mistakes "queued" for "merged". Running it again is harmless. Do not
      delete its branch; GitHub refuses pushes to it until it is taken out of the queue; nobody
      is told if GitHub drops it — ask --status.
  {EXIT_NOT_GREEN_YET}  not yet: a required check has not finished (--no-wait, or the wait ran out), GitHub
      has not caught up with a push, or the PR is leaving the queue right now — run it again later
  {EXIT_CANNOT_ASK}  GitHub could not be asked, or its answer to a sent queue request was lost (the message
      says which) — run it again: a run never queues a PR twice, and finds one that is already queued
  {EXIT_NO_QUEUE}  the PR's target branch has no merge queue (merge it the way PRs are merged today)
  {EXIT_PR_STATE}  the PR cannot be queued as it is: draft, closed, already merged, from a fork, in conflict
      with its target branch, or its target branch changed while this waited
  {EXIT_HEAD}  not the commit that was meant: --sha differs (or GitHub has not caught up with pushing it),
      the PR changed, or commits are not pushed
  {EXIT_CHECKS}  a required check is RED — fix it; running this again will not help. Also when the branch
      requires no check at all: that is a ruleset change, for whoever administers the repository
  {EXIT_GITHUB_REFUSED}  GitHub refused the queue request (its own words are printed)
   {EXIT_PROBE}  the database-schema check refused — its own message says why (prod lacks a column this PR
      declares, or prod could not be asked) — or it could not be run for this commit (not in this checkout)
  {EXIT_USAGE}  the command line could not be understood, or there is no such pull request or repository
 130  stopped (Ctrl-C, TERM, or a hangup outside nohup) — the message says whether the queue request had
      been sent; if it had, ask --status

exit codes for --status (the first word of the first line says the same):
   {STATUS_MERGED}  MERGED — every commit landed
  {EXIT_QUEUED}  QUEUED — in the queue, not landed yet
  {STATUS_NOT_MERGED}  NOT QUEUED / DROPPED / CLOSED — not merged and not in the queue
  {EXIT_CANNOT_ASK}  UNKNOWN — GitHub could not be asked, or shows the PR neither in the queue nor taken out
      of it after 15 s — ask again in a minute
  {EXIT_USAGE}  as above"""

PR_QUERY = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){
  nameWithOwner
  pullRequest(number:$number){
    id number state isDraft baseRefName headRefName headRefOid isCrossRepository reviewDecision mergedAt
    mergeCommit{oid} mergeQueueEntry{state position} mergeable labels(first:50){nodes{name}}
    commits(last:1){nodes{commit{oid statusCheckRollup{contexts(first:100){nodes{__typename
      ... on CheckRun{name status conclusion databaseId startedAt isRequired(pullRequestNumber:$number)}
      ... on StatusContext{context state createdAt isRequired(pullRequestNumber:$number)}}}}}}}
    timelineItems(last:100,itemTypes:[ADDED_TO_MERGE_QUEUE_EVENT,REMOVED_FROM_MERGE_QUEUE_EVENT]){nodes{__typename
      ... on AddedToMergeQueueEvent{createdAt}
      ... on RemovedFromMergeQueueEvent{createdAt reason}}}}}}"""
ENQUEUE_MUTATION = ("mutation($id:ID!,$oid:GitObjectID!){enqueuePullRequest(input:{pullRequestId:$id,"
                    "expectedHeadOid:$oid}){mergeQueueEntry{state position}}}")


class Refusal(Exception):
    """A plain-words reason to stop, with the exit code that names it."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code, self.message = code, message


@dataclass
class Deps:
    """Everything outside this process, so the tests can stand in for it."""
    gh: Callable[[list], "tuple[int, str, str]"]
    out: Callable[[str], None]
    sleep: Callable[[float], None]
    now: Callable[[], float]
    probe_decide: Callable[[str, str, str, str], tuple]
    # (the repository this script's checkout is of — GitHub's own name for it, or None if GitHub could not be
    # asked — and that checkout's root)
    home: Callable[[], "tuple[str | None, str]"]
    # the folder the command was started in, when it is a checkout of `repo`:
    # {"branch": …, "unpushed": [commit lines] or None when it could not be compared}
    local_state: Callable[[str, int, str], "dict | None"]
    ensure_commit: Callable[[str, int, str], bool]


def same_repo(a: "str | None", b: "str | None") -> bool:
    """Both names come from GitHub (`nameWithOwner`), so this is belt and braces: GitHub ignores letter
    case and a trailing `.git` in owner/name."""
    def norm(s):
        return re.sub(r"\.git$", "", (s or "").strip().lower())
    return bool(a) and norm(a) == norm(b)


# ── the command line ───────────────────────────────────────────────────────────────────────────────
def _pr_number(digits: str) -> int:
    n = int(digits)
    if not 1 <= n <= 2_147_483_647:  # GitHub's Int: anything else is an input error, not "could not ask"
        raise ValueError(f"{digits} is not a pull request number")
    return n


def parse_pr(value: str) -> "tuple[int, str | None]":
    """`7`, `#7` or a pull-request URL → (number, repository named by the URL or None)."""
    value = (value or "").strip()
    m = re.fullmatch(r"https://github\.com/([^/\s]+/[^/\s]+)/pull/(\d+)(?:[/?#].*)?", value)
    if m:
        return _pr_number(m.group(2)), m.group(1)
    m = re.fullmatch(r"#?(\d+)", value)
    if m:
        return _pr_number(m.group(1)), None
    raise ValueError(f"{value!r} is not a pull request: give its number (7), #7 in quotes, or its URL")


class _Parser(argparse.ArgumentParser):
    def error(self, message):  # one plain sentence instead of argparse's usage dump + exit 2
        raise Refusal(EXIT_USAGE, f"queue-pr: {message}. Example: python3 scripts/queue-pr.py --pr 123")


def parse_args(argv: list) -> argparse.Namespace:
    p = _Parser(prog="queue-pr.py", formatter_class=argparse.RawDescriptionHelpFormatter,
                description="Put one exact commit of a pull request into GitHub's merge queue: GitHub then tests it\n"
                            "on top of the latest target branch and lands it by itself.\n"
                            f"On success it prints '{QUEUED_LINE}' and exits {EXIT_QUEUED} — queued is not merged.\n"
                            "Afterwards, ask where the PR stands with --status.",
                epilog=EXIT_TABLE)
    p.add_argument("--pr", help="the pull request to queue: its number, or its URL")
    p.add_argument("--status", metavar="PR", help="only say where the PR stands; changes nothing")
    p.add_argument("--sha", help="the commit you mean to queue; refused if the PR is at another commit")
    p.add_argument("--no-wait", action="store_true", help="do not wait for checks that are still running; stop instead")
    p.add_argument("--repo", help="owner/name (default: the repository this script belongs to)")
    p.add_argument("--wait-seconds", type=int, default=None,
                   help="how long to wait for the PR's checks (default: as long as the merge line waits, about 120 min)")
    p.add_argument("--poll-seconds", type=int, default=20, help=argparse.SUPPRESS)
    a = p.parse_args(argv)
    if bool(a.pr) == bool(a.status):
        raise Refusal(EXIT_USAGE, "queue-pr: say which pull request — `--pr 123` to queue it, or `--status 123` "
                                  "to see where it stands. (In a shell, `#123` unquoted is a comment: write 123.)")
    if a.sha is not None and not re.fullmatch(r"[0-9a-fA-F]{7,40}", a.sha):
        raise Refusal(EXIT_USAGE, f"queue-pr: --sha takes a commit id (7 to 40 hex characters), not {a.sha!r}. "
                                  f"`git rev-parse HEAD` prints it.")
    return a


# ── asking GitHub ──────────────────────────────────────────────────────────────────────────────────
def read_pr(repo: str, number: int, deps: Deps) -> dict:
    """GitHub's pull-request object. Raises Refusal (no such PR / could not ask)."""
    owner, _, name = repo.partition("/")
    rc, out, err = deps.gh(["api", "graphql", "-f", f"query={PR_QUERY}", "-f", f"owner={owner}",
                            "-f", f"name={name}", "-F", f"number={number}"])
    try:
        body = json.loads(out) if out.strip() else {}
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    errors = [e for e in (body.get("errors") or []) if isinstance(e, dict)]
    data = body.get("data") if isinstance(body.get("data"), dict) else None
    repository = (data or {}).get("repository") if data else None
    pr = (repository or {}).get("pullRequest")
    # GitHub's real answers (read 2026-10-08): a missing repository is NOT_FOUND at path ["repository"]; a missing
    # PR is NOT_FOUND at ["repository","pullRequest"]. A NOT_FOUND anywhere else (deeper in the object) is a partial
    # answer, not "no such PR" — it must not end a wait or name the PR as missing.
    not_found = {tuple(e.get("path") or ()) for e in errors if e.get("type") == "NOT_FOUND"}
    if ("repository",) in not_found or (not_found and data is not None and repository is None):
        raise Refusal(EXIT_USAGE, f"queue-pr: GitHub does not show a repository named {repo} to this login. "
                                  f"Check the name (--repo owner/name) and `gh auth status`.")
    if ("repository", "pullRequest") in not_found or (rc == 0 and data is not None and not errors and pr is None):
        raise Refusal(EXIT_USAGE, f"queue-pr: there is no pull request #{number} in {repo}.")
    if rc != 0 or errors or not isinstance(pr, dict):  # a partial answer is not an answer
        said = "; ".join(str(e.get("message", "")) for e in errors)
        why = (said or err or out or f"gh exited {rc}").strip()[:400]
        raise Refusal(EXIT_CANNOT_ASK, f"queue-pr: could not ask GitHub about #{number} in {repo} — nothing was "
                                       f"queued. GitHub CLI said: {why}")
    pr["_repo"] = repository.get("nameWithOwner") or repo  # GitHub's own spelling of the name
    return pr


def checks_verdict(pr: dict, head: str, required_names: "list[str]") -> "tuple[str, str]":
    """("green" | "pending" | "red" | "none", what to tell the person) for `head` — and only for `head`."""
    nodes = ((pr.get("commits") or {}).get("nodes")) or []
    commit = (nodes[0].get("commit") if nodes else None) or {}
    contexts = []
    if commit.get("oid") == head:  # a rollup for another commit says nothing about this one
        contexts = (((commit.get("statusCheckRollup") or {}).get("contexts") or {}).get("nodes")) or []
    by_name: "dict[str, list[dict]]" = {}
    for c in contexts:
        by_name.setdefault(c.get("name") or c.get("context") or "?", []).append(c)
    names = list(required_names) + [n for n, cs in by_name.items()
                                    if any(c.get("isRequired") for c in cs) and n not in required_names]
    if not names:
        return "none", "this branch requires no check, so nothing would have tested the commit"
    pending = ""
    for name in names:
        runs = by_name.get(name)
        if not runs:
            pending = pending or f"required check `{name}` has not reported for {head[:9]} yet"
            continue
        # A commit can carry several runs of one check — a cancelled run next to the one that replaced it
        # (live: bookwith-core #4691, `gate` CANCELLED then SUCCESS; GitHub merged it). The NEWEST one is
        # the check's answer, as it is for GitHub. Newest = the higher id: a re-run that has not started yet
        # has no start time, and must still outrank an older green run.
        c = max(runs, key=lambda r: (int(r.get("databaseId") or 0), str(r.get("createdAt") or r.get("startedAt") or "")))
        if c.get("__typename") == "StatusContext":
            state = c.get("state") or "?"
            done, ok, shown = state not in ("PENDING", "EXPECTED"), state == "SUCCESS", state
        else:
            done = c.get("status") == "COMPLETED"
            ok, shown = c.get("conclusion") == "SUCCESS", c.get("conclusion") or c.get("status") or "?"
        if not done:
            pending = pending or f"required check `{name}` is still running ({shown})"
        elif not ok:  # only SUCCESS is green: a skipped or neutral required check tested nothing
            return "red", f"required check `{name}` ended {shown} on {head[:9]}"
    return ("pending", pending) if pending else ("green", "")


def _timeline(pr: dict) -> list:
    return ((pr.get("timelineItems") or {}).get("nodes")) or []


def _queue_events(pr: dict) -> "list[dict]":
    return [it for it in _timeline(pr) if it.get("__typename") in ("AddedToMergeQueueEvent", "RemovedFromMergeQueueEvent")]


def leaving_the_queue(pr: dict) -> bool:
    """GitHub is between "in the queue" and "merged" or "dropped": the queue entry is gone, but the PR is not
    merged yet, and either no removal is recorded yet or the removal says "merged". Seen live (QA walk of
    #4756, cell ST4): for about a second a landing PR reads as not queued, or as dropped with reason "merged"."""
    if pr.get("state") != "OPEN" or pr.get("mergeQueueEntry"):
        return False
    events = _queue_events(pr)
    if not events:
        return False
    last = events[-1]
    return (last.get("__typename") == "AddedToMergeQueueEvent"
            or str(last.get("reason") or "").lower() == "merged")


def _drop_kind(reason: str) -> str:
    """GitHub's drop reason, sorted into what the reader does about it: "conflict", "failed_run" or "other". By
    whole words: GitHub's reasons are snake_case (`failed_checks`, `merge_conflict` — sandbox evidence)."""
    words = set(re.split(r"[^a-z]+", reason.lower()))
    if words & {"conflict", "conflicts", "conflicting"}:
        return "conflict"
    if words & {"fail", "failed", "failure", "failing", "ci", "check", "checks", "timeout", "timed"}:
        return "failed_run"
    return "other"


def _after_drop(reason: str, base: str, number, repo: str) -> str:
    """What to do about a drop, in plain words, by GitHub's reason."""
    kind = _drop_kind(reason)
    if kind == "conflict":
        return f"rebase it on `{base}`, push, get the new commit approved, and queue it again"
    if kind == "failed_run":
        # Not every failed queue run is the PR's fault: the gate's e2e leg fails a queue run whose e2e run GitHub never
        # started, or that was still going at its deadline, and says so (gate-verify-e2e.py; 2b round-2 review).
        return ("fix what failed in its queue run (it was tested on top of the PRs ahead of it), push, get the new "
                "commit approved, and queue it again — unless the queue run's gate error says none of the PR's code "
                "failed (an e2e run that never started or never finished): then the same commit goes back in the "
                "queue as it is, with no push and no new approval")
    # Neither a conflict nor a failed run: a person may have taken it out on purpose (AC4.10, or the owner holding
    # it), and a passing, approved commit queued again MERGES. See who first (product review of #4756, M1).
    return (f"GitHub's reason is neither a conflict nor a failed run — someone may have taken it out on purpose. See "
            f"who, on the PR's page (`gh pr view {number} --web -R {repo}`), before queueing it again")


def status(pr: dict, repo: str, deps: Deps, last_read_failed: "str | None" = None) -> "tuple[list[str], int]":
    """Where the PR stands, from GitHub's own object (AC4.6). The first word is the state; so is the code.
    No "commits pushed after queueing" case exists: GitHub refuses a push to the branch of a queued PR
    ("Branches that are queued for merging cannot be updated" — sandbox evidence row 27)."""
    n, base, head = pr.get("number"), pr.get("baseRefName") or "", pr.get("headRefOid") or ""
    events = _queue_events(pr)
    if pr.get("state") == "MERGED":
        sha = ((pr.get("mergeCommit") or {}).get("oid") or "")[:9]
        owed = ("the deploy check; then clean up the worktree and branch; then the lane row"
                if not _repo_flag(repo, deps) else f"whatever {repo} does after a merge")
        return [f"MERGED #{n} into {base} as {sha} ({pr.get('mergedAt')}).", f"  Owed now: {owed}."], STATUS_MERGED
    entry = pr.get("mergeQueueEntry")
    if entry:
        since = next((e.get("createdAt") for e in reversed(events) if e.get("__typename") == "AddedToMergeQueueEvent"), "?")
        return [f"QUEUED #{n} for {base} — position {entry.get('position')}, {entry.get('state')}, since {since}. "
                f"It has not landed: do not delete its branch. GitHub refuses pushes to it until it is taken out of "
                f"the queue."], EXIT_QUEUED
    if leaving_the_queue(pr):  # main asked again first (settle): still no entry and no removal — say so, do not guess
        # e.g. the queue rule removed at the rollback switch (fresh-pair review of #4756, M5)
        seen = (f"GitHub could not be asked on the last try ({last_read_failed[:120]})" if last_read_failed else
                f"GitHub shows it neither in the queue nor taken out of it after {SETTLE_POLL_S * SETTLE_TRIES} s")
        return [f"UNKNOWN #{n} for {base} — {seen}. It may be landing slowly, or the queue rule was removed while it "
                f"was queued. Ask again in a minute: python3 scripts/queue-pr.py --status {n}{_repo_flag(repo, deps)}"
                ], EXIT_CANNOT_ASK
    if pr.get("state") == "CLOSED":
        return [f"CLOSED #{n} — it was closed without landing."], STATUS_NOT_MERGED
    dropped = events[-1] if events and events[-1].get("__typename") == "RemovedFromMergeQueueEvent" else None
    state = None
    if pr.get("isDraft"):
        why = "it is still a draft"
    else:
        state, info = queue_state(repo, base, deps.gh)
        if state == "none":
            why = f"`{base}` has no GitHub queue — it lands the way PRs are merged today"
        elif state == "unknown":
            why = f"could not ask GitHub whether `{base}` has a queue"
        elif pr.get("mergeable") == "CONFLICTING":
            why = (f"it conflicts with `{base}` — rebase it, push, and get the new commit approved; no check runs "
                   f"while it conflicts")
        elif pr.get("reviewDecision") == "CHANGES_REQUESTED":
            why = "a reviewer asked for changes — it needs a new commit and a new approval"
        elif pr.get("reviewDecision") == "REVIEW_REQUIRED":
            why = f"waiting for a QA approval of {head[:9]}"
        else:
            verdict, detail = checks_verdict(pr, head, list(info))
            why = (f"ready — queue it with `python3 scripts/queue-pr.py --pr {n}{_repo_flag(repo, deps)}`"
                   if verdict == "green" else detail)
    if dropped:
        # Whether a fix was pushed since the drop cannot be read reliably: GitHub's timeline places a commit by
        # its commit time, not its push time (QA walk of #4756, cell D2). So say what is known, and where it is now.
        reason = str(dropped.get("reason") or "unknown")
        if why.startswith("ready"):
            # The PR's own checks passing says nothing about the queue run that failed: offering "queue it" here
            # re-queued the very commit that had just been dropped (live re-walk of #4756, cell D1).
            why = ("its own checks pass, which does not clear the drop — queue it again only once the step above is "
                   "done (a new commit needs a new approval)")
            if _drop_kind(reason) == "failed_run":  # a flaky test is a reason to re-queue only a failed run
                why += ", or once that failure is shown to be a known flaky test"
        if state == "none":
            step = f"`{base}` has no GitHub queue any more, so it lands the way PRs are merged today"
        elif state == "unknown":  # cto L7: do not say "queue it again" when whether a queue exists is not known
            step = f"could not ask GitHub whether `{base}` still has a queue — find that out before queueing it again"
        else:
            step = _after_drop(reason, base, n, repo)
        return [f"DROPPED #{n} from the queue for {base} at {dropped.get('createdAt')} — GitHub's reason: {reason}. "
                f"It is not queued now. Next, on branch `{pr.get('headRefName')}`: {step}.",
                f"  Now (at {head[:9]}): {why}."], STATUS_NOT_MERGED
    return [f"NOT QUEUED #{n} (target {base}, at {head[:9]}): {why}."], STATUS_NOT_MERGED


# ── the run ────────────────────────────────────────────────────────────────────────────────────────
def set_label(repo: str, number: int, on: bool, deps: Deps) -> None:
    if on:
        rc, _, err = deps.gh(["api", "-X", "POST", f"repos/{repo}/issues/{number}/labels", "-f", f"labels[]={WAIT_LABEL}"])
    else:
        rc, _, err = deps.gh(["api", "-X", "DELETE", f"repos/{repo}/issues/{number}/labels/{WAIT_LABEL}"])
    if rc != 0 and on:
        deps.out(f"  (could not add the `{WAIT_LABEL}` label — continuing without it: {(err or '').strip()[:120]})")
    elif rc != 0 and "Label does not exist" not in (err or ""):
        deps.out(f"  ⚠️  could not take the `{WAIT_LABEL}` label off #{number} — remove it by hand: {(err or '').strip()[:120]}")


def _repo_flag(repo: str, deps: Deps) -> str:
    """` --repo owner/name` when the PR is not in this script's own repository: a printed next step must act
    on the PR it is about, wherever it is pasted."""
    return "" if same_repo(deps.home()[0], repo) else f" --repo {repo}"


def refuse_unqueueable(pr: dict, number: int, repo: str) -> None:
    if pr.get("state") == "MERGED":
        raise Refusal(EXIT_PR_STATE, f"queue-pr: #{number} is already merged — nothing to queue.")
    if pr.get("state") != "OPEN":
        raise Refusal(EXIT_PR_STATE, f"queue-pr: #{number} is closed — nothing was queued. Reopen it first.")
    if pr.get("isDraft"):
        raise Refusal(EXIT_PR_STATE, f"queue-pr: #{number} is a draft — nothing was queued. Mark it ready for review "
                                     f"first (`gh pr ready {number} -R {repo}`).")
    if pr.get("isCrossRepository"):
        raise Refusal(EXIT_PR_STATE, f"queue-pr: #{number} comes from a fork — nothing was queued. This command queues "
                                     f"branches of the repository itself only.")
    # GitHub runs no check on a conflicting PR: waiting would never end. A PR already in the queue is answered as
    # queued instead — GitHub can mark it CONFLICTING for a moment when main moves (fresh-pair review of #4756, M1).
    if pr.get("mergeable") == "CONFLICTING" and not pr.get("mergeQueueEntry"):
        raise Refusal(EXIT_PR_STATE, f"queue-pr: #{number} conflicts with its target branch — nothing was queued. "
                                     f"Rebase it, push, and get the new commit approved.")


def _labels(pr: dict) -> "list[str]":
    return [str(n.get("name")) for n in ((pr.get("labels") or {}).get("nodes") or []) if isinstance(n, dict)]


def wait_for_green(repo: str, number: int, base: str, head: str, required: "list[str]", a, deps: Deps,
                   first_detail: str) -> dict:
    """Return the PR once its required checks are green for `head` (or someone else queued it); raise Refusal
    otherwise. The label is on only while this waits, and is taken off on every way out."""
    budget = a.wait_seconds if a.wait_seconds is not None else default_wait_seconds()
    deps.out(f"  {first_detail}. Waiting up to {max(1, budget // 60)} min for the checks of {head[:9]}; the label "
             f"`{WAIT_LABEL}` is on the PR while this waits.\n"
             f"  If this run is stopped, NOTHING is queued — run it again. (Agents: a foreground call ends after "
             f"10 minutes; run this in the background, or use --no-wait and come back.)")
    started, detail, unanswered = deps.now(), first_detail, None
    try:
        # inside the try: a run stopped WHILE the label is being added must still take it off
        set_label(repo, number, True, deps)
        while True:
            if deps.now() - started > budget:
                if unanswered is not None:  # the last thing we know is that GitHub did not answer
                    raise Refusal(EXIT_CANNOT_ASK, f"queue-pr: GitHub stopped answering while waiting for #{number} — "
                                                   f"nothing was queued. Run it again. Last error: {unanswered}")
                raise Refusal(EXIT_NOT_GREEN_YET, f"queue-pr: #{number} was not green after {max(1, budget // 60)} min "
                                                  f"({detail}) — nothing was queued. Run it again when the checks are green.")
            deps.sleep(a.poll_seconds)
            try:
                pr = read_pr(repo, number, deps)
                unanswered = None
            except Refusal as r:
                if r.code != EXIT_CANNOT_ASK:
                    raise
                unanswered = r.message
                continue  # a failed poll is neither green nor red: ask again, the budget still bounds it
            if pr.get("headRefOid") != head:
                new = str(pr.get("headRefOid"))[:9]
                raise Refusal(EXIT_HEAD, f"queue-pr: #{number} changed while waiting — {new} was pushed after this run "
                                         f"started on {head[:9]}. Nothing was queued. {new} needs its own QA approval "
                                         f"before it is queued.")
            refuse_unqueueable(pr, number, repo)
            if pr.get("baseRefName") != base:
                raise Refusal(EXIT_PR_STATE, f"queue-pr: #{number}'s target branch changed while waiting ({base} → "
                                             f"{pr.get('baseRefName')}) — nothing was queued. Run it again.")
            if pr.get("mergeQueueEntry"):
                return pr  # someone else queued it meanwhile; the caller reports that
            verdict, detail = checks_verdict(pr, head, required)
            if verdict == "green":
                return pr
            if verdict in ("red", "none"):
                raise Refusal(EXIT_CHECKS, f"queue-pr: {detail} — nothing was queued. Fix it; running this again will not help.")
    finally:
        set_label(repo, number, False, deps)


def already_queued(pr: dict, number: int, base: str, deps: Deps) -> int:
    deps.out(f"{QUEUED_LINE} — #{number} is already in the queue for {base} "
             f"(position {(pr.get('mergeQueueEntry') or {}).get('position')}). Nothing more to do; ask --status later.")
    return EXIT_QUEUED


def _escape_reason(owed) -> str:
    return str(getattr(owed, "reason", "") or "(no reason recorded)")


def note_escape(repo: str, number: int, head: str, owed, deps: Deps, later: bool = False) -> None:
    """The owner's schema-probe escape leaves a mark on the PR at QUEUE time — worded as queued, because the
    merge line's own record says "Merged with …" and a queued PR is not merged."""
    files = ", ".join(f"`{f}`" for f in tuple(getattr(owed, "schema_files", ()) or ())[:12]) or "(not listed)"
    body = (f"⚠️ **Queued (not merged) at `{head[:9]}` with the prod-schema probe SKIPPED** — prod's catalog was not "
            f"asked whether every column this PR declares exists (the #755 outage shape).\n\n"
            + ("_Recorded by a later run that found it already queued: the run that queued it may have lost GitHub's "
               "reply before writing this._\n\n" if later else "")
            + f"**Reason given:** {_escape_reason(owed)}\n\n**Schema files:** {files}\n\n"
            f"Audit query: `gh pr list --state all --label {ESCAPE_LABEL}`.")
    deps.gh(["api", "-X", "POST", f"repos/{repo}/issues/{number}/labels", "-f", f"labels[]={ESCAPE_LABEL}"])
    rc, _, err = deps.gh(["api", "-X", "POST", f"repos/{repo}/issues/{number}/comments", "-f", f"body={body}"])
    if rc != 0:
        deps.out(f"  ⚠️  could not write the escape's comment on #{number}: {(err or '').strip()[:160]}")


def _escape_owed(repo: str, number: int, head: str, deps: Deps) -> "tuple[object, str | None]":
    """(the escape's record to write, or None; why a record that may be owed could NOT be written, or None)."""
    home_repo, home_root = deps.home()
    if not home_repo:
        return None, "this script's checkout could not be named, so the escape could not be checked"
    if not same_repo(home_repo, repo):
        return None, None  # no probe applies to another repository's PR
    if not deps.ensure_commit(home_root, number, head):
        return None, f"commit {head[:9]} is not in this checkout ({home_root})"
    verdict, message, owed = deps.probe_decide(home_root, str(number), head, "queue")
    if verdict == "skip" or owed is not None:
        return owed, None
    if verdict == "refuse":
        return None, f"the escape does not apply to #{number} ({str(message)[:160]})"
    return None, None


def queued_already(repo: str, pr: dict, number: int, base: str, head: str, deps: Deps,
                   escape=None, decided: bool = False) -> int:
    """The PR is in the queue already. If the owner's schema-probe escape is in effect and the PR carries no record
    of it, the run that queued it lost GitHub's reply before writing one — write it now, or the PR lands with the
    probe skipped and nothing on record (fresh-pair review of #4756, M3; BUG-453's shape). `decided`: this run
    already ran the probe decision, so `escape` is its answer and the probe is not asked twice."""
    if ESCAPE_LABEL not in _labels(pr):
        if not decided and os.environ.get("SKIP_PROD_SCHEMA_PROBE"):
            escape, why_not = _escape_owed(repo, number, head, deps)
            if why_not:  # loud, as the queueing path is for the same conditions (cto review, M2)
                deps.out(f"  ⚠️  the owner's schema-probe escape is set, but NO record was written on #{number}: "
                         f"{why_not}. If it was queued with the probe skipped, add the label by hand "
                         f"(gh pr edit {number} -R {repo} --add-label {ESCAPE_LABEL}) and a comment with the reason.")
        if escape is not None:
            note_escape(repo, number, head, escape, deps, later=True)
    return already_queued(pr, number, base, deps)


def queue(a, number: int, repo: str, pr: dict, deps: Deps, run: dict) -> int:
    """`run` records how far this run got (`sent` = the queue request left this process), so an interrupted
    run can say what it knows instead of claiming that nothing was queued."""
    stale_label = WAIT_LABEL in _labels(pr)  # left by an earlier run that was killed; this run cleans it up
    try:
        return _queue(a, number, repo, pr, deps, run)
    except Refusal as r:  # every refusal names the PR and the branch it targets (AC4.1)
        if r.message.startswith("queue-pr: "):
            r.message = f"queue-pr (#{number} into {pr.get('baseRefName')}): " + r.message[len("queue-pr: "):]
        raise
    finally:
        if stale_label:
            set_label(repo, number, False, deps)


def _queue(a, number: int, repo: str, pr: dict, deps: Deps, run: dict) -> int:
    base, head = pr.get("baseRefName") or "", pr.get("headRefOid") or ""
    refuse_unqueueable(pr, number, repo)
    state, info = queue_state(repo, base, deps.gh)
    if state == "unknown":
        raise Refusal(EXIT_CANNOT_ASK, f"queue-pr: could not ask GitHub whether `{base}` has a merge queue — nothing "
                                       f"was queued. Run it again. GitHub CLI said: {info}")
    if state != "live":
        today = (f"use /request-merge (or today's merge line, python3 scripts/merge-driver.py --pr {number} "
                 f"--branch {pr.get('headRefName')})" if not _repo_flag(repo, deps) else f"merge it the way {repo} merges today")
        raise Refusal(EXIT_NO_QUEUE, f"queue-pr: no GitHub queue on {base} — nothing was queued. #{number} lands the way "
                                     f"PRs are merged today: {today}.")
    required = list(info)
    if a.sha and not head.lower().startswith(a.sha.lower()):
        raise Refusal(EXIT_HEAD, f"queue-pr: #{number} is at {head[:9]}, not the commit you named ({a.sha[:9]}) — "
                                 f"nothing was queued. If you just pushed {a.sha[:9]}, GitHub has not caught up yet: "
                                 f"run it again in a few seconds. Otherwise {a.sha[:9]} is not this PR's commit, or "
                                 f"{head[:9]} was pushed after it — and {head[:9]} needs its own QA approval.")
    if pr.get("mergeQueueEntry"):
        return queued_already(repo, pr, number, base, head, deps)
    if leaving_the_queue(pr):  # the ~1 s while GitHub lands or drops it (sandbox row 28): a request now would race it
        raise Refusal(EXIT_NOT_GREEN_YET, f"queue-pr: #{number} is leaving the queue right now (GitHub is landing or "
                                          f"dropping it) — nothing was queued by this run. Ask in a few seconds: "
                                          f"python3 scripts/queue-pr.py --status {number}{_repo_flag(repo, deps)}")
    local = None if a.sha else deps.local_state(repo, number, head)
    if local and local.get("branch") == pr.get("headRefName"):
        if local.get("unpushed") is None:
            raise Refusal(EXIT_HEAD, f"queue-pr: could not compare this folder's branch `{local.get('branch')}` with "
                                     f"#{number}'s commit {head[:9]} — nothing was queued. Run `git fetch origin` here, "
                                     f"or name the commit you mean with --sha.")
        if local["unpushed"] and local.get("remote_head") and local.get("remote_head") == local.get("head"):
            # the branch on GitHub already has this folder's commit — only the PR has not caught up with the push
            raise Refusal(EXIT_NOT_GREEN_YET, f"queue-pr: GitHub has not caught up with the push yet (the PR still shows "
                                              f"{head[:9]}; the branch has {str(local['head'])[:9]}) — nothing was "
                                              f"queued. Run it again in a few seconds.")
        if local["unpushed"]:
            raise Refusal(EXIT_HEAD, f"queue-pr: this folder has commits that are not pushed to #{number} "
                                     f"(GitHub has {head[:9]}):\n    " + "\n    ".join(local["unpushed"]) +
                                     "\n  Push them first, or name the commit you mean with --sha. Nothing was queued.")
    home_repo, home_root = deps.home()
    if not home_repo:  # never "skip" the probe because a name could not be read: that is how a schema PR slips through
        raise Refusal(EXIT_CANNOT_ASK, f"queue-pr: could not ask GitHub which repository this script's checkout "
                                       f"({home_root}) is a clone of, so it cannot tell whether #{number} needs the "
                                       f"database-schema check — nothing was queued. Check `gh auth status` and run it again.")
    escape = None
    if same_repo(home_repo, repo):
        if not deps.ensure_commit(home_root, number, head):
            raise Refusal(EXIT_PROBE, f"queue-pr: commit {head[:9]} is not in this checkout ({home_root}), so its schema "
                                      f"cannot be checked — nothing was queued. Run `git fetch origin pull/{number}/head` there.")
        verdict, message, owed = deps.probe_decide(home_root, str(number), head, "queue")
        if verdict == "refuse":
            deps.out(f"  {message}")
            raise Refusal(EXIT_PROBE, f"queue-pr: the database-schema check refused #{number} — nothing was queued.")
        if verdict == "skip" or owed is not None:
            escape = owed
            deps.out(f"  ⚠️  prod-schema probe SKIPPED via SKIP_PROD_SCHEMA_PROBE — reason: {_escape_reason(owed)}. "
                     f"If GitHub accepts the queue request, #{number} gets the label `{ESCAPE_LABEL}` and a comment.")
        else:
            deps.out(f"  {message}")
    else:
        deps.out(f"  prod-schema probe: #{number} is in {repo}, not this script's own repository "
                 f"({home_repo}) — no probe applies.")
    verdict, detail = checks_verdict(pr, head, required)
    if verdict in ("red", "none"):
        raise Refusal(EXIT_CHECKS, f"queue-pr: {detail} — nothing was queued. Fix it; running this again will not help.")
    if verdict == "pending":
        if a.no_wait:
            raise Refusal(EXIT_NOT_GREEN_YET, f"queue-pr: {detail} — nothing was queued (--no-wait). Run it again when it is green.")
        waited = wait_for_green(repo, number, base, head, required, a, deps, detail)
        if waited.get("mergeQueueEntry"):
            return queued_already(repo, waited, number, base, head, deps, escape, decided=True)
    run["sent"] = True  # from here on, an interrupted run cannot know whether GitHub queued the PR
    run["where"] = (f"#{number} into {base}", f"python3 scripts/queue-pr.py --status {number}{_repo_flag(repo, deps)}")
    rc, out, err = deps.gh(["api", "graphql", "-f", f"query={ENQUEUE_MUTATION}", "-f", f"id={pr.get('id')}", "-f", f"oid={head}"])
    try:
        answer = json.loads(out) if (out or "").strip() else {}
    except ValueError:
        answer = {}
    errors = [e for e in ((answer.get("errors") if isinstance(answer, dict) else None) or []) if isinstance(e, dict)]
    entry = (((answer.get("data") or {}).get("enqueuePullRequest") or {}).get("mergeQueueEntry")
             if isinstance(answer, dict) and isinstance(answer.get("data"), dict) else None)
    if rc != 0 or errors or not entry:
        # Do not believe an exit code: ask GitHub where the PR stands now. Someone else may have queued it in
        # the meantime, or GitHub queued it and only the reply was lost — neither is a refusal.
        said = "; ".join(str(e.get("message", "")) for e in errors)
        # GitHub's read side lags a write by seconds (sandbox evidence row 30: 11 s): ask over that window before
        # concluding anything — also after error text, whose shape for an internal error is not measured.
        for attempt in range(READBACK_TRIES):
            if attempt:
                deps.sleep(READBACK_POLL_S)
            try:
                now = read_pr(repo, number, deps)
            except Refusal:
                raise Refusal(EXIT_CANNOT_ASK,
                              f"queue-pr: the queue request for #{number} was sent, but GitHub's answer was lost and the "
                              f"PR could not be read back — it is NOT KNOWN whether it was queued. Check with: "
                              f"python3 scripts/queue-pr.py --status {number} --repo {repo}") from None
            if now.get("mergeQueueEntry") or (now.get("headRefOid") and now.get("headRefOid") != head):
                break
        if now.get("mergeQueueEntry") and now.get("headRefOid") == head:
            if escape is not None and ESCAPE_LABEL not in _labels(now):  # a parallel run may have written it
                note_escape(repo, number, head, escape, deps)
            return already_queued(now, number, base, deps)
        if now.get("headRefOid") and now.get("headRefOid") != head:
            new = str(now.get("headRefOid"))[:9]
            if now.get("mergeQueueEntry"):  # another session queued the newer commit (fresh-pair review, L8)
                raise Refusal(EXIT_HEAD, f"queue-pr: #{number} moved to {new} while it was being queued, and {new} is "
                                         f"in the queue now — someone else queued it; this run queued nothing. Make sure "
                                         f"{new} was approved; if it was not, take it out of the queue.")
            raise Refusal(EXIT_HEAD, f"queue-pr: #{number} moved to {new} while it was being queued (GitHub was asked for "
                                     f"{head[:9]}) — nothing was queued. {new} needs its own QA approval before it is queued.")
        if not said:
            raise Refusal(EXIT_CANNOT_ASK,
                          f"queue-pr: the queue request for #{number} got no answer from GitHub, and GitHub does not show "
                          f"it in the queue (asked {READBACK_TRIES} times over {READBACK_POLL_S * (READBACK_TRIES - 1)} s). "
                          f"Run it again: a run never queues a PR twice, and finds one that is already queued. "
                          f"GitHub CLI said: {(err or out or '').strip()[:300]}")
        raise Refusal(EXIT_GITHUB_REFUSED,
                      f"queue-pr: GitHub refused to queue #{number} for {base} — nothing was queued. GitHub said: {said}\n"
                      f"  If it asks for a review or an approval: ask a QA session to walk and approve {head[:9]}.\n"
                      f"  If it names a conflict: rebase the branch, push, and get the new commit approved.\n"
                      f"  If a check was still running: run this again when it is green.")
    if escape is not None:
        note_escape(repo, number, head, escape, deps)
    here = "" if same_repo(home_repo, repo) else f" --repo {repo}"
    deps.out(f"{QUEUED_LINE} — GitHub lands #{number} into {base} at {head[:9]} when its queue run passes.\n"
             f"  Do not delete its branch. GitHub refuses pushes to it while it is queued; to change it, take it out "
             f"of the queue first. Nobody is told if GitHub drops it.\n"
             f"  See where it stands: python3 scripts/queue-pr.py --status {number}{here}")
    return EXIT_QUEUED


def main(argv: list, deps: "Deps | None" = None) -> int:
    deps = deps or real_deps()
    run: dict = {}
    number = None
    try:
        a = parse_args(argv)
        try:
            number, url_repo = parse_pr(a.pr or a.status)
        except ValueError as e:
            raise Refusal(EXIT_USAGE, f"queue-pr: {e}.")
        if a.repo and url_repo and not same_repo(a.repo, url_repo):
            raise Refusal(EXIT_USAGE, f"queue-pr: the URL names {url_repo} but --repo says {a.repo} — which one? "
                                      f"Give one of them.")
        if not (a.repo or url_repo) and not deps.home()[0]:
            raise Refusal(EXIT_CANNOT_ASK, "queue-pr: could not ask GitHub which repository this checkout is — nothing "
                                           "was queued. Check `gh auth status`, or name it with --repo owner/name.")
        repo = a.repo or url_repo or deps.home()[0]
        if not repo or "/" not in repo:
            raise Refusal(EXIT_USAGE, "queue-pr: say which repository with --repo owner/name.")
        try:
            pr = read_pr(repo, number, deps)
        except Refusal as r:
            if a.status and r.code == EXIT_CANNOT_ASK:  # --status: the first word is the state — here, UNKNOWN
                said = r.message.split("GitHub CLI said:", 1)[-1].strip()
                raise Refusal(EXIT_CANNOT_ASK, f"UNKNOWN #{number} — GitHub could not be asked ({said[:160]}). Ask "
                                               f"again in a minute.") from None
            raise
        repo = pr.get("_repo") or repo  # from here on, GitHub's own spelling
        if a.status:
            last_read_failed = None
            for _ in range(SETTLE_TRIES):  # a PR leaving the queue settles within seconds: ask again before answering
                if not leaving_the_queue(pr):
                    break
                deps.sleep(SETTLE_POLL_S)
                try:
                    pr = read_pr(repo, number, deps)
                    last_read_failed = None
                except Refusal as r:
                    last_read_failed = r.message.split("GitHub CLI said:", 1)[-1].strip()
            lines, code = status(pr, repo, deps, last_read_failed)
            for line in lines:
                deps.out(line)
            return code
        return queue(a, number, repo, pr, deps, run)
    except Refusal as r:
        deps.out(r.message)
        return r.code
    except KeyboardInterrupt:
        if run.get("sent"):
            which, ask = run.get("where") or (f"#{number}", f"python3 scripts/queue-pr.py --status {number}")
            deps.out(f"queue-pr: stopped after the queue request for {which} was sent — it is NOT KNOWN whether "
                     f"GitHub queued it. Check with: {ask}")
        else:
            deps.out("queue-pr: stopped — nothing was queued by this run.")
        return 130


# ── the real outside world ─────────────────────────────────────────────────────────────────────────
GH_TIMEOUT_S = 60    # no single call to GitHub may hang the run (or hold the label on) longer than this
GIT_TIMEOUT_S = 120  # a fetch of the PR's commit can take a while on a large repository


def _gh(args: list, cwd: "str | None" = None) -> "tuple[int, str, str]":
    try:
        r = subprocess.run(["gh", *args], capture_output=True, text=True, cwd=cwd, timeout=GH_TIMEOUT_S)
    except FileNotFoundError:
        return 127, "", "the GitHub CLI (`gh`) is not installed"
    except subprocess.TimeoutExpired:
        return 124, "", f"GitHub did not answer within {GH_TIMEOUT_S} s"
    return r.returncode, r.stdout, r.stderr


def _git(args: list, root: str) -> "tuple[int, str]":
    try:
        r = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, timeout=GIT_TIMEOUT_S)
    except (FileNotFoundError, NotADirectoryError, subprocess.TimeoutExpired):
        return 127, ""
    return r.returncode, r.stdout.strip()


def _github_repo_of(root: str) -> "str | None":
    """GitHub's own name for the repository whose clone `root` is — the repository its `origin` remote points
    at. Asked of GitHub with origin's URL, never parsed from it (an ssh alias, a port or another letter case
    would make a parsed name differ from the PR's). NOT a bare `gh repo view`: that picks `upstream`, or a
    `gh repo set-default` choice, over `origin` — and `origin` is where the schema probe reads its commits."""
    rc, url = _git(["remote", "get-url", "origin"], root)
    if rc != 0 or not url:
        return None
    rc, out, _ = _gh(["repo", "view", url, "--json", "nameWithOwner", "-q", ".nameWithOwner"], cwd=root)
    name = out.strip()
    return name if rc == 0 and "/" in name else None


_MERGE_DRIVER = None


def _merge_driver():
    """merge-driver.py, loaded as a module (hyphenated name ⇒ importlib): its schema decision and its gate
    wait are the oracles — asked, never copied (CLAUDE.md rule 15)."""
    global _MERGE_DRIVER
    if _MERGE_DRIVER is None:
        spec = importlib.util.spec_from_file_location("_merge_driver_for_queue_pr", os.path.join(HERE, "merge-driver.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        _MERGE_DRIVER = mod
    return _MERGE_DRIVER


def default_wait_seconds() -> int:
    """As long as the merge line waits for the gate: the job's cap plus the time before a run exists.
    That number is pinned to gate.yml by test_merge_driver_gate_timeout.py — read it, do not restate it."""
    try:
        return int(_merge_driver().GATE_TIMEOUT_DEFAULT_SECONDS)
    except Exception:  # noqa: BLE001 — a missing driver must not turn a wait into a crash
        return 7200


def real_deps(home_root: "str | None" = None, cwd: "str | None" = None, name_of=None) -> Deps:
    home_root = home_root or os.path.dirname(HERE)  # the checkout this script lives in — NOT where it was started
    cwd = cwd or os.getcwd()
    name_of = name_of or _github_repo_of

    def ensure_commit(root: str, number: int, sha: str) -> bool:
        if _git(["cat-file", "-e", f"{sha}^{{commit}}"], root)[0] == 0:
            return True
        _git(["fetch", "-q", "origin", f"pull/{number}/head"], root)  # reads into FETCH_HEAD; moves no branch
        return _git(["cat-file", "-e", f"{sha}^{{commit}}"], root)[0] == 0

    def local_state(repo: str, number: int, pr_head: str) -> "dict | None":
        rc, branch = _git(["rev-parse", "--abbrev-ref", "HEAD"], cwd)
        if rc != 0:
            return None  # not a checkout at all
        here = name_of(cwd)
        if here is None:  # could not tell which repository this is: the caller refuses if the branch matches the PR's
            return {"branch": branch, "unpushed": None}
        if not same_repo(here, repo):
            return None  # a checkout of another repository: nothing here could be "unpushed" to this PR
        if not ensure_commit(cwd, number, pr_head):
            return {"branch": branch, "unpushed": None}  # cannot compare — the caller refuses, it does not guess
        rc, unpushed = _git(["log", "--format=%h %s", f"{pr_head}..HEAD"], cwd)
        head = _git(["rev-parse", "HEAD"], cwd)[1]
        rc_remote, remote = _git(["ls-remote", "origin", f"refs/heads/{branch}"], cwd)
        remote_head = remote.split()[0] if rc_remote == 0 and remote.split() else None
        return {"branch": branch, "unpushed": unpushed.splitlines() if rc == 0 else None,
                "head": head, "remote_head": remote_head}

    return Deps(gh=_gh, out=lambda s: print(s, flush=True), sleep=time.sleep, now=time.monotonic,
                probe_decide=lambda r, pr, head, stage: _merge_driver().prod_schema_decision(r, pr, head, stage),
                home=functools.lru_cache(maxsize=1)(lambda: (name_of(home_root), home_root)),
                local_state=local_state, ensure_commit=ensure_commit)


STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def _stopped(_signum, _frame):
    # One stop is enough: ignore any further one, so a second Ctrl-C or TERM cannot cut the cleanup short.
    for s in STOP_SIGNALS:
        signal.signal(s, signal.SIG_IGN)
    raise KeyboardInterrupt  # so the label comes off and the run says what it knows


def install_stop_handlers() -> None:
    """A closed terminal (HUP) or a tool's timeout (TERM) is how a long wait usually ends: treat both like
    Ctrl-C. A signal that is already ignored stays ignored — `nohup` means "do not stop on HUP"."""
    for s in STOP_SIGNALS:
        if signal.getsignal(s) is not signal.SIG_IGN:
            signal.signal(s, _stopped)


if __name__ == "__main__":
    install_stop_handlers()
    sys.exit(main(sys.argv[1:]))
