#!/usr/bin/env python3
"""check-merged-pr-qa.py — after a PR merges, did the QA account approve what merged? (native-merge-queue SPEC AC4.9)

WHY. Once `main` has GitHub's merge queue, GitHub refuses to queue an app PR without the QA team's approval
(AC5.0a test 1). But it does NOT check again once a PR is queued: on the sandbox, a QA approval dismissed while
the PR sat in the queue did not stop the merge (test 4, PR #70 merged two minutes later). So the after-merge look is
the only net for that case, and for an approval nobody can account for. This is that look. It reads GitHub's own
record and posts what it found; it never reverts, re-queues or closes anything.

WHAT IT CHECKS, for a merged PR (run by .github/workflows/merged-pr-audit.yml on `pull_request_target: closed`):
  1. Does the base branch have a merge queue? GitHub's rules for the branch (scripts/_queue_live.py — the one call).
     No queue → "nothing to audit": today's merges carry their own record (the merge line's labels).
     A queue with no reviewer that needs an approval → UNGUARDED on the default branch (GitHub enforces no QA
     there at all); on another branch (a rehearsal branch before AC5.1 adds its review rule) only a warning.
     Every required reviewer counts as the QA team: AC5.2 has exactly one. A second would need this to learn
     which team is QA.
  2. Did the PR touch a file a REQUIRED reviewer covers? Each reviewer's patterns are read from the same rules
     (pull_request rule, `required_reviewers` with at least one approval) — what GitHub itself enforced. GitHub's
     ruleset help gives their format as "the same as a standard .gitignore file", read in order, a `!` pattern
     unmatching earlier ones (its REST reference says "fnmatch syntax"; the two differ, and the help page is the one
     that describes ordering and `!`). So the reading is asked of git itself (`git check-ignore`), the reference
     implementation of that format — never re-implemented here — and the deciding pattern is named in the reason.
     Whether GitHub reads every pattern as git does is measured at the rehearsal (SPEC AC5.1). A rename counts by
     its old name too. Nothing covered → nothing owed.
  3. The QA account's (scripts/qa_review.py's QA_LOGIN) LATEST review that takes a position — APPROVED,
     CHANGES_REQUESTED, or one since DISMISSED — decides: a later "changes requested" withdraws an approval, as on
     GitHub. A dismissed review counts here as the latest word, which may be stricter than GitHub (its docs say a
     dismissed review becomes a comment, so an older approval might stand again): an alarm GitHub would not raise,
     never a silence. qa-review.sh never posts a second approval on one commit, so the case needs a person.
     It must be an APPROVED review of the merged head. GitHub moves an approval to the new head across an
     identical-diff push or rebase (AC5.0a test 2), so the commit check holds for a carried approval too; the
     marker then names the walked commit.
       · its first line is qa-review.sh's APPROVED marker for this PR (qa_review.parse_marker, never a copy) →
         QA-VERDICT, label `qa-verdict-external`;
       · it starts with `OWNER OVERRIDE` (any case: Ori types it on a phone) → OWNER-OVERRIDE, label
         `qa-signoff-skipped`, comment "merged on the owner's override" (a record, not an alert — AC4.3);
       · neither → ALERT: an approval nobody can account for.
     No standing approval → ALERT, naming any QA review a PERSON dismissed (timeline: `dismissalMessage` set;
     GitHub's own dismissal on a code change has none — AC5.0a test 5).
  4. On every app PR it audits, a comment "merged <sha> — deploy check owed". The workflow's health job asks
     production's `/ready` (the database too) 10 minutes later.
A failed read is retried twice (5 s, then 20 s) before it counts as unread.

EXIT: 0 NOTHING / QA-VERDICT / OWNER-OVERRIDE · 1 ALERT / UNGUARDED / COULD-NOT-ASK (the workflow's alert job opens
or updates ONE `qa-audit-alert` issue that @-mentions Ori). The verdict and reason go to $GITHUB_OUTPUT.
Env: GITHUB_REPOSITORY, PR_NUMBER, BASE_REF, DEFAULT_BRANCH, HEAD_SHA (the PR's head at merge), MERGE_SHA, GH_TOKEN.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from _queue_live import branch_rules, required_reviewer_patterns  # noqa: E402  — the one rules call


def _load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, file))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


QR = _load("_qa_review_for_audit", "qa_review.py")  # QA_LOGIN and parse_marker: asked, never copied

NOTHING, QA_VERDICT, OVERRIDE = "NOTHING", "QA-VERDICT", "OWNER-OVERRIDE"
ALERT, UNGUARDED, UNREAD = "ALERT", "UNGUARDED", "COULD-NOT-ASK"
VERDICT_LABEL, OVERRIDE_LABEL = "qa-verdict-external", "qa-signoff-skipped"
OVERRIDE_PREFIX = "OWNER OVERRIDE"
OVERRIDE_RE = re.compile(r"OWNER OVERRIDE(?![A-Za-z0-9_])", re.IGNORECASE)
POSITIONS = ("APPROVED", "CHANGES_REQUESTED", "DISMISSED")  # a COMMENTED review changes nobody's position
RETRY_WAITS = (5, 20)

DISMISSALS_QUERY = """query($owner:String!,$name:String!,$number:Int!){repository(owner:$owner,name:$name){
pullRequest(number:$number){timelineItems(last:100,itemTypes:[REVIEW_DISMISSED_EVENT]){nodes{... on ReviewDismissedEvent{
createdAt actor{login} dismissalMessage previousReviewState review{databaseId author{login} commit{oid}}}}}}}}"""


class Unread(Exception):
    pass


def gh(args: list[str]) -> tuple[int, str, str]:
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    return r.returncode, r.stdout, r.stderr


def patient(gh_call, sleep):
    """gh_call that tries a failed READ again after 5 s and 20 s: one GitHub hiccup must not raise a false alarm."""
    def call(args: list[str]) -> tuple[int, str, str]:
        for wait in (*RETRY_WAITS, None):
            rc, out, err = gh_call(args)
            if rc == 0 or wait is None:
                return rc, out, err
            sleep(wait)
        raise AssertionError("unreachable")
    return call


def covered_by(patterns: list[str], paths: list[str]) -> dict[str, str]:
    """{path: the deciding pattern} for every path the ordered list covers, read the .gitignore way — asked of
    `git check-ignore` in an empty scratch repository, never re-implemented. Case-sensitive, as on GitHub's Linux
    (only a Mac's `git init` would set core.ignoreCase); no global, system or template config can add a pattern.
    Each path goes in as `./<path>`, so a name starting with `:` is a name, never git pathspec magic (`:/x`, `:!x`),
    and in bytes, so a `\r` in a name survives."""
    if not paths:
        return {}
    if any(c in p for p in patterns for c in "\n\r\0") or any("\0" in p for p in paths):
        raise Unread("a file pattern or path holds a line break or NUL — cannot be read the .gitignore way")
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}  # a caller's GIT_DIR must not redirect it
    # What pins the reading is the two `-c` flags and `--template=` below (a mutant honouring the caller's global
    # config stays green: those keys are overridden). Dropping system and global config is belt and braces.
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    with tempfile.TemporaryDirectory() as tmp:
        excludes, repo = os.path.join(tmp, "patterns"), os.path.join(tmp, "repo")
        with open(excludes, "w", encoding="utf-8") as fh:
            fh.write("".join(p + "\n" for p in patterns))
        try:
            init = subprocess.run(["git", "init", "-q", "--template=", repo], capture_output=True, text=True, env=env)
            r = subprocess.run(["git", "-C", repo, "-c", "core.ignoreCase=false", "-c", f"core.excludesFile={excludes}",
                                "check-ignore", "--no-index", "--verbose", "--non-matching", "-z", "--stdin"],
                               input=b"".join(("./" + p).encode("utf-8", "surrogateescape") + b"\0" for p in paths),
                               capture_output=True, env=env)
        except OSError as e:
            raise Unread(f"could not run git to read the file patterns ({e})") from None
        except UnicodeError:
            raise Unread("a file name could not be passed to git as bytes") from None
    if init.returncode != 0 or r.returncode not in (0, 1):
        said = init.stderr if init.returncode != 0 else r.stderr.decode("utf-8", "replace")
        raise Unread(f"git could not read the file patterns ({said.strip()[:200]})")
    fields = r.stdout.decode("utf-8", "surrogateescape").split("\0")[:-1]
    if len(fields) != 4 * len(paths):
        raise Unread("git's reading of the file patterns did not answer for every path")
    out = {}
    for i in range(0, len(fields), 4):
        _source, _line, pattern, path = fields[i:i + 4]
        if pattern and not pattern.startswith("!"):
            out[path[2:] if path.startswith("./") else path] = pattern
    return out


def ask(gh_call, args: list[str], what: str):
    rc, out, err = gh_call(args)
    if rc != 0:
        raise Unread(f"{what}: gh exited {rc} ({(err or out).strip()[:200]})")
    try:
        return json.loads(out)
    except ValueError:
        raise Unread(f"{what}: GitHub's answer was not JSON") from None


def every_page(gh_call, path: str, what: str) -> list:
    out, page = [], 1
    while True:
        data = ask(gh_call, ["api", f"{path}?per_page=100&page={page}"], what)
        if not isinstance(data, list):
            raise Unread(f"{what}: GitHub's answer was not a list")
        out += data
        if len(data) < 100:
            return out
        page += 1


def touched_paths(files: list) -> list[str]:
    paths = []
    for f in files:
        for key in ("filename", "previous_filename"):
            p = f.get(key) if isinstance(f, dict) else None
            if isinstance(p, str) and p not in paths:
                paths.append(p)
    return paths


def person_dismissals(gh_call, repo: str, number: int, head: str) -> list[str]:
    """QA reviews on the merged head that a PERSON dismissed (a message; GitHub's own dismissal has none)."""
    owner, name = repo.split("/", 1)
    data = ask(gh_call, ["api", "graphql", "-f", f"query={DISMISSALS_QUERY}", "-F", f"owner={owner}",
                         "-F", f"name={name}", "-F", f"number={number}"], "reading the PR's dismissals")
    nodes = (((data.get("data") or {}).get("repository") or {}).get("pullRequest") or {}).get("timelineItems") or {}
    out = []
    for n in nodes.get("nodes") or []:
        review = (n or {}).get("review") or {}
        if ((review.get("author") or {}).get("login") == QR.QA_LOGIN and n.get("dismissalMessage")
                and ((review.get("commit") or {}).get("oid") or head) == head):
            out.append(f"{(n.get('actor') or {}).get('login') or 'someone'} dismissed the QA account's "
                       f"{(n.get('previousReviewState') or 'review').lower()} at {n.get('createdAt')}: "
                       f"\"{n['dismissalMessage'][:200]}\"")
    return out


def standing(reviews: list) -> dict | None:
    """The QA account's latest review that takes a position (POSITIONS), as GitHub weighs a reviewer: newest wins."""
    mine = [r for r in reviews if (r.get("user") or {}).get("login") == QR.QA_LOGIN and r.get("state") in POSITIONS]
    return max(mine, key=lambda r: (r.get("submitted_at") or "", r.get("id") or 0)) if mine else None


def audit(env: dict[str, str], gh_call, sleep=time.sleep) -> dict:
    """{exit, verdict, reason, label, comments} — decided only; main() writes the label and comments."""
    gh_call = patient(gh_call, sleep)
    repo, base, default = env.get("GITHUB_REPOSITORY", ""), env.get("BASE_REF", ""), env.get("DEFAULT_BRANCH", "")
    head, merge = env.get("HEAD_SHA", ""), env.get("MERGE_SHA", "")
    try:
        number = int(env.get("PR_NUMBER", ""))
    except ValueError:
        number = 0
    if not (repo and base and head and number):
        return {"exit": 1, "verdict": UNREAD, "reason": "GITHUB_REPOSITORY / PR_NUMBER / BASE_REF / HEAD_SHA missing"}
    state, rules = branch_rules(repo, base, gh_call)
    if state != "ok":
        return {"exit": 1, "verdict": UNREAD, "reason": f"could not read the rules on `{base}` ({rules})"}
    if not any(r.get("type") == "merge_queue" for r in rules):
        return {"exit": 0, "verdict": NOTHING, "reason": f"no merge queue on `{base}` — nothing to audit (merges there "
                                                        f"carry the merge line's own record)"}
    pattern_sets = required_reviewer_patterns(rules)
    if not pattern_sets:
        unguarded = f"`{base}` has a merge queue but no required reviewer that needs an approval — GitHub enforces no QA"
        # A rehearsal branch has a queue and, until AC5.1 adds it, no review rule: not an alarm. An unknown default
        # branch keeps the alarm (a lost env value must not silence main).
        if default and base != default:
            return {"exit": 0, "verdict": NOTHING, "warning": True,
                    "reason": f"{unguarded} there; not the default branch (`{default}`), so not an alarm"}
        return {"exit": 1, "verdict": UNGUARDED, "reason": f"{unguarded} on the default branch at all"}
    if any(not patterns for patterns in pattern_sets):
        return {"exit": 1, "verdict": UNREAD, "reason": f"a required reviewer on `{base}` lists no file pattern — what "
                                                        f"GitHub requires then is not documented, so what was owed "
                                                        f"cannot be read"}
    try:
        rows = every_page(gh_call, f"repos/{repo}/pulls/{number}/files", "reading the PR's files")
        paths = touched_paths(rows)
        covered: dict[str, str] = {}
        for patterns in pattern_sets:
            covered = {**covered_by(patterns, paths), **covered}
        if not covered:
            return {"exit": 0, "verdict": NOTHING, "reason": f"#{number} touches no file the QA rule on `{base}` covers"}
        first = sorted(covered)[0]
        owed = f"{len(covered)} covered file(s), e.g. {first} (pattern `{covered[first]}`)"
        reviews = every_page(gh_call, f"repos/{repo}/pulls/{number}/reviews", "reading the PR's reviews")
        last = standing(reviews)
        deploy = f"merged {merge[:9] or head[:9]} — deploy check owed (`/deploy-check`); the health job asks prod in 10 min."
        if last and last.get("state") == "APPROVED" and last.get("commit_id") == head:
            mark = QR.parse_marker(last.get("body"))
            if mark and mark["verdict"] == QR.APPROVED and mark["pr"] == number and mark.get("owner") == "chat":
                # CONSENT_ATTRIBUTION_OK: meta — describes the record a pass-backed review carries, claims no approval
                # owner-chat-approve AC-7: Ori answered `approve` in a session's chat for this head; the review lists what
                # the walk did not test. His decision, so a record — never an alert, whatever the walk covered.
                return {"exit": 0, "verdict": OVERRIDE, "label": OVERRIDE_LABEL, "owner_kind": "chat",
                        "comments": [f"Merged on the owner's approval in chat (review {last.get('id')}): "
                                     f"the merged head {head[:9]} carries it; what the walk did not test is listed in that review. "
                                     f"A record, not an alert.", deploy],
                        "reason": f"owner approval in chat (review {last.get('id')}); {owed}"}
            if mark and mark["verdict"] == QR.APPROVED and mark["pr"] == number and mark.get("walked_by") == "builder":
                # main's copy re-checks what the PR branch's own qa_review.py decided (merge-queue-simplify, WG-2)
                risky = QR.builder_walk_refused_rows(rows)
                if risky:
                    return {"exit": 1, "verdict": ALERT, "comments": [deploy],
                            "reason": f"the QA account's approval of {head[:9]} (review {last.get('id')}) was posted on a "
                                      f"builder-driven walk, but #{number} changes payments, login or the database "
                                      f"schema ({', '.join(risky[:4])}) — that needs the QA runner's own walk or the "
                                      f"owner's override; {owed}"}
            if mark and mark["verdict"] == QR.APPROVED and mark["pr"] == number:
                carried = (f" (walked at {mark['sha'][:9]}; GitHub carried the approval to {head[:9]} over an "
                           f"identical-diff push)") if mark["sha"] != head else ""
                return {"exit": 0, "verdict": QA_VERDICT, "label": VERDICT_LABEL, "comments": [deploy],
                        "walked_by": mark.get("walked_by"),  # the QA digest says who walked it (owner-chat-approve AC-10)
                        "reason": f"the QA account approved {head[:9]} through qa-review.sh (review {last.get('id')})"
                                  f"{carried}; {owed}"}
            if OVERRIDE_RE.match((last.get("body") or "").lstrip()):
                return {"exit": 0, "verdict": OVERRIDE, "label": OVERRIDE_LABEL,
                        "comments": [f"Merged on the owner's override (review {last.get('id')}): no QA walk backs "
                                     f"{head[:9]}. A record, not an alert.", deploy],
                        "reason": f"OWNER OVERRIDE approval (review {last.get('id')}); {owed}"}
            return {"exit": 1, "verdict": ALERT, "comments": [deploy],
                    "reason": f"the QA account's approval of {head[:9]} (review {last.get('id')}) carries neither "
                              f"qa-review.sh's APPROVED marker for #{number} nor `{OVERRIDE_PREFIX}` — nobody can "
                              f"account for it; {owed}"}
        if last and last.get("state") == "CHANGES_REQUESTED":
            state = f"the QA account's latest review (review {last.get('id')}) REQUESTED CHANGES"
        elif last and last.get("state") == "APPROVED":
            state = f"the QA account's latest approval is of {str(last.get('commit_id'))[:9]}, not the merged head"
        else:
            state = "the QA account has no standing approval"
        why = "; ".join(person_dismissals(gh_call, repo, number, head))
        return {"exit": 1, "verdict": ALERT, "comments": [deploy],
                "reason": f"#{number} merged {head[:9]} with NO standing QA approval of that commit — {state}"
                          + (f"; {why}" if why else "") + f"; {owed}"}
    except Unread as e:
        return {"exit": 1, "verdict": UNREAD, "reason": str(e)}


def command_data(text: str) -> str:
    """A value made safe for a `::level::` workflow command: GitHub's own escapes, so a line break in a reason cannot
    end the annotation early or start a second command."""
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def main(env: dict[str, str] | None = None, gh_call=gh, sleep=time.sleep) -> int:
    env = dict(os.environ) if env is None else env
    # A file name or a dismissal message can hold a character with no UTF-8 form (a lone surrogate): printed, it must
    # read as an escape, never end the run in a traceback after the label and comment were written.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(errors="backslashreplace")
    result = audit(env, gh_call, sleep)
    repo, number = env.get("GITHUB_REPOSITORY", ""), env.get("PR_NUMBER", "")
    level = "error" if result["exit"] else "warning" if result.get("warning") else "notice"
    print(f"::{level} title=merged-PR audit {result['verdict']}::{command_data(result['reason'])}")
    # The plain line carries text from outside (file names, a dismissal's message, gh's errors). Printed inside
    # stop-commands, so neither a `::` line nor the runner's older `##[…]` form ANYWHERE in it is read as a command.
    resume = secrets.token_hex(16)
    print(f"::stop-commands::{resume}")
    print(f"merged-PR audit: {result['verdict']} — {' '.join(result['reason'].splitlines())}")
    print(f"::{resume}::")
    # Writes are NOT retried (a retried comment can post twice); a failed one is a warning, never the verdict.
    for args in ([["pr", "edit", number, "-R", repo, "--add-label", result["label"]]] if result.get("label") else []) + \
            [["pr", "comment", number, "-R", repo, "--body", body] for body in result.get("comments") or []]:
        rc, _out, err = gh_call(args)
        if rc != 0:
            print(f"::warning title=merged-PR audit::{command_data(f'could not {args[0]} {args[1]} on #{number} ({err.strip()[:200]})')}")
    out = env.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8", errors="backslashreplace") as fh:
            fh.write(f"verdict={result['verdict']}\n")
            fh.write(f"reason={' '.join(result['reason'].splitlines())}\n")
    return result["exit"]


if __name__ == "__main__":
    sys.exit(main())
