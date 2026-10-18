#!/usr/bin/env python3
"""qa_review.py — the QA account's only voice on GitHub (native-merge-queue SPEC AC4.2).

Run it through `bash scripts/qa-review.sh`, which reads the QA account's token from the OS secret store and
hands it over on file descriptor 3, a pipe, never in the environment. An environment is inherited by every child
(the prod-schema probe runs the PR's own schema code; `gh` and `git` run as children) and stays readable by every
process of the same user for the process's whole life (`ps -E`), so a token there could reach code under review
(fresh cto review of #5011, M1; QA of #5011, F1). A direct run may still pass QA_REVIEW_TOKEN; main() pops it before
anything runs. The token is never printed (a token with characters no GitHub token has is refused before any
request carries it, and errors print only their type), never written anywhere, and never sent to a redirect.

WHAT IT DOES
  --check
      The token is there, it belongs to the QA account, the account has Write on the repository, and it can
      read the repository's pull requests. The QA skills run this BEFORE a walk starts. One thing it cannot
      see: whether the token itself may WRITE reviews (GitHub does not report a fine-grained token's scopes);
      a refused post exits 5 with how to mint a token that can. Prints the token's expiry, and warns within
      14 days.
  <pr> --approve --sha <walked sha> --evidence <verdict file> [--report-url <link>]
      Refuses unless the verdict's HEADER (the verdict line and the two lines after it) names THIS PR and
      THIS commit in full, says APPROVED and Release: GO, and every cell was walked: live + test-fidelity ==
      total, with blocked-accepted, rationalized and missing all 0 (each of the five counts must be there — a
      missing count is never read as zero). Refuses unless the commit is the PR's head and the PR is open and
      ready. Refuses when the QA account has a "changes requested" review on that commit, or ANY dismissed
      review on it: the first verdict on a commit stands, and a dismissal is a person's (or the base's) word
      this command must not talk over. Runs the prod-schema probe for it. Posts an APPROVE review PINNED to
      that commit (`commit_id`, which `gh pr review` cannot set), whose first line is the marker the
      merged-PR audit (AC4.9) reads. Then runs the queue command for that commit, naming that review (a read that
      lags behind the post is then left to GitHub, never refused).
  <pr> --request-changes --sha <walked sha> --evidence <verdict file> [--report-url <link>]
      The same checks for a CHANGES-REQUIRED verdict; posts REQUEST_CHANGES carrying it. Refused when the
      QA account already approved that commit.

THE EVIDENCE FILE is the qa-specialist's own verdict, whose header block is fixed by
.claude/agents/qa-specialist.md and read ONLY there (a field elsewhere in the file never counts):
    QA-SIGNOFF VERDICT: APPROVED
    Feature: <slug>   PR: #<N>   Head SHA: <full sha>   Walked by: <builder|qa-agent|qa-runner>   Release: GO
    Coverage: <walked>/<total> cells  (live: n, test-fidelity: n, blocked-accepted: n, rationalized: n, missing: n)
A blocked cell is the owner's decision to make, so such a PR goes through the owner override (AC4.3), not
through this file.

WHO WALKED (merge-queue-simplify Slice B). The header says who drove the browser; an approval without it is refused.
An independent qa-specialist audit that APPROVED a builder-driven walk (the /qa-signoff path) is posted — EXCEPT
for a PR that changes payments, login or the database schema (QA_RUNNER_PATHS): that needs the QA runner's or a
fresh QA agent's own walk, or Ori's OWNER OVERRIDE. The field is the audit's word, not a proof (option A's threat
model is mistakes, not an adversary); the marker records it (`walked-by=`) for the merged-PR audit.

EXIT CODES
  0 done · 2 refused, nothing posted (the message says why and what to do) · 3 no usable token (missing,
  malformed, expired, revoked — or not handed over by qa-review.sh: the message says which, and only the first
  kinds need a new token) · 4 the token is not the QA account's · 5 the account or token may not see or
  act on the repository · 10 approved AND queued or armed (not merged) · 12 GitHub could not be asked · 30 the review IS
  posted but queueing did not finish (the queue command's own words above say why; running
  `python3 scripts/queue-pr.py --pr N --sha S --repo OWNER/NAME` again never queues twice) · 64 usage.
"""
from __future__ import annotations

import argparse
import errno
import hashlib
import importlib.util
import json
import os
import re
import select
import signal
import stat
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

HERE = os.path.dirname(os.path.abspath(__file__))
HOME_ROOT = os.path.dirname(HERE)
QA_LOGIN = "bookwith-qa"            # the account in team qa-approvers (owner-setup.md §3)
TOKEN_ENV = "QA_REVIEW_TOKEN"       # a direct run's way in; popped from the environment before anything runs
TOKEN_FD_ENV = "QA_REVIEW_TOKEN_FD" # set by qa-review.sh (not secret): the token is on this file descriptor
QUEUE_STOP_GRACE_S = 20            # a stopped queue command gets this long to take its wait label off. Its own
                                   # gh calls may wait 60 s, so a slow label removal can be cut off: the label is
                                   # then left on, and `queue-pr.py --status` still tells the truth.
TOKEN_MAX_BYTES = 4096              # a GitHub token is well under 300 characters
TOKEN_READ_S = 10                   # the wrapper's printf ends at once; a pipe nobody closes must not hang the run
STOP_POLL_S = 0.2                   # how often the queue runner looks for a stop while the queue command runs
API = "https://api.github.com"
API_TIMEOUT_S = 30
BODY_CAP = 60_000                   # GitHub's review-body limit is 65,536 characters
EXPIRY_WARN_DAYS = 14
MINT_HELP = ("Ask Ori to mint a new token as the QA account (docs/features/native-merge-queue/owner-setup.md §3: "
             "fine-grained, owner BookwithOrg, repositories bookwith-core + mq-sandbox, Pull requests: read and "
             "write, Contents: read) and to store it on EACH Mac with: security add-generic-password -a github-qa "
             "-s claude-code-mcp -U -w   (paste the token at the prompt — it is not shown; never paste it into a "
             "chat). On Linux, scripts/mcp-helpers/secret-backend.sh names the store.")
_TOKEN_SHAPE = re.compile(r"[A-Za-z0-9_]+")

EXIT_OK, EXIT_REFUSED, EXIT_NO_TOKEN, EXIT_WRONG_ACCOUNT, EXIT_NO_ACCESS = 0, 2, 3, 4, 5
EXIT_QUEUED, EXIT_CANNOT_ASK, EXIT_POSTED_NOT_QUEUED, EXIT_USAGE = 10, 12, 30, 64

APPROVED, CHANGES = "APPROVED", "CHANGES-REQUIRED"
WALKERS = ("builder", "qa-agent", "qa-runner")
MARKER_RE = re.compile(r"^QA-REVIEW v1 verdict=(?P<verdict>APPROVED|CHANGES-REQUIRED) pr=(?P<pr>\d+) "
                       r"sha=(?P<sha>[0-9a-f]{40}) cells=(\d+)/(\d+) findings=C(?P<C>\d+)/H(?P<H>\d+)/M(?P<M>\d+)/L(?P<L>\d+)"
                       r"(?: walked-by=(?P<walked_by>builder|qa-agent|qa-runner))?(?: owner=(?P<owner>chat))?"
                       r" report=(?P<report>\S+)$")
COUNTS = ("live", "test-fidelity", "blocked-accepted", "rationalized", "missing")
# A builder-driven walk (the /qa-signoff path) that an independent audit APPROVED is posted, EXCEPT where the PR changes
# one of these: payments, login, the database schema (merge-queue-simplify AC-B3; Ori's three words, 2026-10-18). They
# need the QA runner's (or a fresh QA agent's) own walk, or Ori's OWNER OVERRIDE. Narrower than merge-driver's
# HIGH_RISK_RE on purpose: that one also matches booking, member, attendance, session — most app PRs.
QA_RUNNER_PATHS = re.compile(
    # payments — Takbull, HYP, Grow, pay links, issued documents (invoices / receipts), money records
    r"[Pp]ayment|(?<!ga4-)[Pp]urchase(?![Ss]equence|[-_][Ss]equence|[Aa]utomation|[Ss]end)|[Tt]akbull|[Hh]yp(?=[A-Z_.-])"
    r"|[Pp]ay[-_]?[Ll]ink|[Pp]ayout|[Cc]heckout|[Rr]efund|[Ii]nvoice|[Rr]eceipt|[Bb]ill(?!ingBlock)|[Cc]harge|[Ss]ubscription"
    r"|[Ii]nstallment|[Gg]iftCard|[Ww]allet|[Ff]inancial|(?:^|/|[a-z])[Gg]row(?=[A-Z_.-])"
    r"|[Ii]ssueDocument|[Cc]redit|(?:^|/)[a-z]+-join\.tsx$|[Ss]ave[-_]?[Cc]ard|[Cc]ard[Cc]ollection|(?:^|/)[Pp]rocessor[A-Z]"
    r"|[Mm]ark[-_]?[Pp]aid|[Dd]ebt|[Mm]oney|[Dd]eposit|[Cc]laim[-_]?[Ss]ession|[Ii]nvite[-_]?[Tt]oken"
    # login — not "Authored"/"Authoring" (message templates), not OAuth / re-auth / Instagram login (connecting Meta,
    # Google: integrations, not how people sign in to BookWith)
    r"|(?<![Oo])(?<![Rr]e)[Aa]uth(?![a-z])|[Aa]uthenticat|[Aa]uthori[sz]|[Cc]lerk|^server/middleware/"
    r"|(?<![Ii]nstagram)[Ll]ogin|[Ss]ign[-_]?[Ii]n"
    r"|(?:^|[/_.-])otp(?=[A-Z_.-])|[a-z]Otp(?=[A-Z_.-])"
    # the code that sends and checks those codes, the owner's phone proof (the identity key) and the session's CSRF
    # guard (QA audit round 2, finding A: the otp files were refused, their phone-verification siblings were not)
    r"|[Pp]hone[-_]?[Vv]erif|(?:^|/)csrf\.ts$"
    # the customer sign-in route and its landing page (round 3, R3-A: #1493, an account-takeover fix, changed only the
    # route), the WhatsApp claim token and its store, the owner's phone reclaim. test_qa_review's census test keeps this
    # family honest: every file that creates a session or sends / checks a sign-in code must be on it.
    r"|^server/routes/client/index\.ts$|(?:^|/)client-verify\.tsx$|[Cc]laim[-_]?[Tt]oken|[Bb]earer[-_]?[Tt]oken"
    r"|[Pp]hone[-_]?[Rr]eclaim"
    # database schema
    r"|^shared/schema/|(?:^|/)migrations/|(?:^|/)drizzle\.config\.ts$"
)
# Only code counts: the app's roots, never a test, a translation or a doc (a docs fragment named after its branch
# "fix-ig-force-reauth" must not turn a PR into a login PR — fresh product + CTO review of #5236)
_CODE_ROOT = re.compile(r"^(?:client|server|shared|migrations)/|^drizzle\.config\.ts$")
_NOT_RISK = re.compile(r"(?:^|/)__tests__/|\.(?:test|spec)\.[^/]+$|/i18n/locales/|\.md$")

_VERDICT_LINE = re.compile(r"^[ \t>]*QA-SIGNOFF VERDICT:\s*(APPROVED|CHANGES-REQUIRED)\s*$")
_PR_RE = re.compile(r"\bPR:\s*#(\d+)\b")
_SHA_RE = re.compile(r"\bHead SHA:\s*`?([0-9a-fA-F]+)`?")
_RELEASE_RE = re.compile(r"\bRelease:\s*(GO|NO-GO)\s*$")  # last on its line: "GO | NO-GO" (the template) is neither
# never the template's own "builder | qa-agent | qa-runner" (it would read as builder — QA charter B-3)
_WALKED_RE = re.compile(r"\bWalked by:\s*(builder|qa-agent|qa-runner)(?![\w-])(?!\s*\|)")
_COVERAGE_RE = re.compile(r"^[ \t>]*Coverage:\s*(\d+)\s*/\s*(\d+)\s*cells\b(.*)$")
# The qa-specialist's own count of its open findings, on the header line after Coverage (or after Spend). Ori's in-chat
# approval reads THIS, never a count guessed from the findings' prose (QA charter of #5278, FW-1: three rounds found the
# prose scan incomplete).
_OPEN_FINDINGS_RE = re.compile(r"^[ \t>]*Open findings:\s*C(\d+)\s*/\s*H(\d+)\s*/\s*M(\d+)\s*/\s*L(\d+)\s*$")
_FINDINGS_HEAD = re.compile(r"^\s*(?:\[Findings\]|#{1,6}\s*Findings\b)", re.I)
_SECTION_HEAD = re.compile(r"^\s*(?:\[[^\]]+\]|#{1,6}\s)")
_FINDING_RE = re.compile(r"^\s*[-*]\s*\**\s*(?:SEVERITY:\s*)?(Critical|High|Medium|Low)\**\s*(?:—|–|-|:)\s*(.*)$", re.I)
_FULL_SHA = re.compile(r"[0-9a-f]{40}")
# CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism; claims no approval
# A backstop for Ori's in-chat approval, behind the declared `Open findings:` line: a verdict that DECLARES C0/H0 but
# lists a Critical / High is refused too. Real verdicts write findings many ways (`- High · CONFIRMED …`, `1. **🐛 HIGH /
# …`, `| High | … |`, `• High — …`, a `## High` heading under `# Findings`), so it scans every listed line of the
# findings section and goes by the FIRST severity word on it (`- Low — the high-water mark` is a Low). It fails closed:
# no findings section refuses, and only a line that says nothing but "none" (or "High: 0") is read as none (fresh CTO
# review of #5278 round 2 HIGH-A; QA charter FW-1: `1. Critical — none of the receipts …` was read as "none").
_OWNER_FINDINGS_HEAD = re.compile(r"^\s*(?:\[Findings\]|(#{1,6})\s*\**\s*Findings\b|\*\*\s*Findings\b|Findings\s*:)", re.I)
_OWNER_NEXT_SECTION = re.compile(r"^\s*(?:\[(?!Findings)[^\]]+\]|[#*\s]*(?:Verdict basis|BLOCKED|Owed before|How it ships|"
                                 r"What I checked|Summary)\b)", re.I)
_MD_HEADING = re.compile(r"^\s*(#{1,6})\s")
_OWNER_LISTED = re.compile(r"^\s*(?:[-*+•]\s|\d+[.)]\s|#{1,6}\s|\*\*|\|)")
_SEVERITY_WORD = re.compile(r"\b(critical|blocker|high|medium|low)\b(?!-level)", re.I)
_LIST_MARKUP = re.compile(r"^[\s\-*+•|#>_`]*(?:\d+[.)])?[\s*_`]*")
_NONE_SAID = re.compile(r"^(?:none|nothing|n/?a|no\s+(?:open\s+)?(?:critical\s*/\s*high|critical|high)\s+findings?"
                        r"|(?:critical|high)\s*(?:[:=]\s*0|\(\s*0\s*\)))[\s.!*_|]*$", re.I)


def owner_findings_problems(text: str) -> list[str]:
    # CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism; claims no approval
    """Why Ori's in-chat approval may NOT cover this verdict's findings (empty: it may). Fail closed."""
    lines = text.splitlines()
    starts = [(i, m.group(1)) for i, l in enumerate(lines) if (m := _OWNER_FINDINGS_HEAD.match(l))]
    if not starts:
        return ["it has no findings section to check against (`[Findings]`) — ask the qa-specialist to re-issue it "
                "in the template"]
    for start, hashes in starts:
        for line in lines[start + 1:]:
            if _OWNER_NEXT_SECTION.match(line):
                break
            rest = _LIST_MARKUP.sub("", line, count=1).strip()
            first = _SEVERITY_WORD.search(rest) if _OWNER_LISTED.match(line) else None
            if first and first.group(1).lower() in ("critical", "blocker", "high") and not _NONE_SAID.match(rest):
                return [f"its findings name a Critical / High item («{line.strip()[:90]}») — an in-chat approval "
                        f"covers checks that could not be tested, never found bugs: fix it and walk the new head"]
            heading = _MD_HEADING.match(line)
            if heading and hashes and len(heading.group(1)) <= len(hashes):  # a sibling section: the findings ended
                break
    return []


class Interrupted(Exception):
    """The queue step was stopped (SIGTERM — an agent's tool timeout sends it —, Ctrl-C, or a closed terminal)."""


# The stop the queue step received, as a RECORD, never a raised exception: a handler that raised could land between
# the queue command's start and the wait for it, and leave that command running with nobody watching (fresh cto
# review of f20ecaa090, M1). review() sets it from its handler; a queue runner reads it with stop_requested().
_STOP: list[int] = []
# The running queue command's pid, while there is one: the handler passes a stop on to it AT ONCE, so a harness that
# follows a pid-only SIGTERM with a SIGKILL inside the poll interval cannot leave it running unwatched (QA of
# 59f0c9a4ed, F1). The poll still passes the stop on when the handler came before the pid was known.
_CHILD: list[int] = []


def stop_requested() -> int | None:
    """The signal that asked the queue step to stop, or None."""
    return _STOP[0] if _STOP else None


def _say(text: str) -> None:
    """To stderr, and nothing else: a hung-up terminal (the SIGHUP case) must not turn exit 30 into a traceback. A
    stderr that is GONE (a broken pipe, a hung-up terminal, a closed descriptor) is swapped for /dev/null, or Python's
    own flush at exit fails and exits 120 (QA of f20ecaa090); anything else (a full non-blocking pipe, a re-entrant
    write) loses only this one line (cto review of 59f0c9a4ed, L3)."""
    try:
        print(text, file=sys.stderr, flush=True)
    except OSError as e:
        if e.errno in (errno.EPIPE, errno.EIO, errno.EBADF):
            try:
                sys.stderr = open(os.devnull, "w")
            except OSError:
                pass
    except RuntimeError:
        pass


class Stop(Exception):
    """A refusal or a failure, with the exit code and the words the caller prints."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Evidence:
    verdict: str
    pr: int | None
    sha: str | None
    release: str | None
    walked: int | None
    total: int | None
    counts: dict[str, int | None] = field(default_factory=dict)
    findings: dict[str, int] = field(default_factory=dict)
    walked_by: str | None = None
    open_findings: dict[str, int] | None = None  # the `Open findings:` line; None when the header has none


def _findings(lines: list[str]) -> dict[str, int]:
    """Severity-tagged bullets inside the Findings section only; a bullet saying 'none' is not a finding."""
    out, inside = {"C": 0, "H": 0, "M": 0, "L": 0}, False
    for line in lines:
        if _FINDINGS_HEAD.match(line):
            inside = True
            continue
        if inside and _SECTION_HEAD.match(line):
            inside = False
        m = _FINDING_RE.match(line) if inside else None
        if m and not re.match(r"(?i)none\b", m.group(2).strip()):
            out[m.group(1)[0].upper()] += 1
    return out


def parse_evidence(text: str) -> Evidence:
    """The verdict HEADER the qa-specialist writes: exactly one verdict line, then the Feature/PR/Head SHA
    line, then the Coverage line. Fields anywhere else in the file are never read (fresh cto review, M3)."""
    lines = text.splitlines()
    at = [i for i, line in enumerate(lines) if _VERDICT_LINE.match(line)]
    if len(at) != 1:
        raise Stop(EXIT_REFUSED, f"the evidence must carry exactly one `QA-SIGNOFF VERDICT:` line; it carries {len(at)}")
    i = at[0]
    verdict = _VERDICT_LINE.match(lines[i]).group(1)
    ids = lines[i + 1] if i + 1 < len(lines) else ""
    cov_line = lines[i + 2] if i + 2 < len(lines) else ""
    pr, sha, release, walked_by = _PR_RE.search(ids), _SHA_RE.search(ids), _RELEASE_RE.search(ids), _WALKED_RE.search(ids)
    cov = _COVERAGE_RE.match(cov_line)
    declared = next((m for line in lines[i + 3:i + 5] if (m := _OPEN_FINDINGS_RE.match(line))), None)
    counts: dict[str, int | None] = {}
    for name in COUNTS:
        m = re.search(rf"(?<![\w-]){re.escape(name)}:\s*(\d+)", cov.group(3)) if cov else None
        counts[name] = int(m.group(1)) if m else None
    return Evidence(verdict=verdict, pr=int(pr.group(1)) if pr else None, sha=sha.group(1) if sha else None,
                    release=release.group(1) if release else None,
                    walked=int(cov.group(1)) if cov else None, total=int(cov.group(2)) if cov else None,
                    counts=counts, findings=_findings(lines), walked_by=walked_by.group(1) if walked_by else None,
                    open_findings=dict(zip("CHML", map(int, declared.groups()))) if declared else None)


def evidence_problems(ev: Evidence, want: str, pr: int, head: str, owner: bool = False) -> list[str]:
    """Why this verdict cannot back a `want` review of `pr` at `head` (empty: it can). The four refusals of
    check-qa-signoff.py (missing, another commit, another PR, not approved) plus the coverage rule. Fails
    closed: a field the header does not carry is a problem, never a zero.

    (CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism or labels its record; claims no approval)
    `owner`: Ori approved THIS verdict for THIS head in the session's chat (owner-chat-approve; the pass is checked by
    the caller). His decision covers what was not walked — blocked, rationalized or missing cells, and a verdict held at
    CHANGES-REQUIRED / NO-GO for them — so those are not problems; who walked it, the PR, the commit and the coverage
    line still are (the review records them, and the question he answered named them)."""
    problems = []
    if ev.verdict != want and not (owner and want == APPROVED and ev.verdict == CHANGES):
        problems.append(f"the verdict says {ev.verdict}, not {want}")
    if ev.pr is None:
        problems.append("its header does not name the PR (`PR: #<N>` on the line after the verdict)")
    elif ev.pr != pr:
        problems.append(f"it is for PR #{ev.pr}, not #{pr}")
    if ev.sha is None or not _FULL_SHA.fullmatch(ev.sha):
        problems.append("its header does not name the walked commit in full (`Head SHA:` with all 40 lower-case "
                        "hex characters)")
    elif ev.sha != head:
        problems.append(f"it is for commit {ev.sha[:9]}, not {head[:9]}")
    if want == APPROVED:
        if ev.walked_by is None:  # fail closed: a missing walker is never read as a QA agent
            problems.append("its header does not say who walked it (`Walked by: builder`, `qa-agent` or `qa-runner` on "
                            "the line after the verdict) — ask the qa-specialist to re-issue its verdict with it; never "
                            "edit the verdict file")
        if ev.release != "GO" and not owner:
            problems.append(f"its header says Release: {ev.release or '(none)'}, not GO")
        if ev.walked is None or ev.total is None:
            problems.append("its header has no `Coverage: <walked>/<total> cells` line (two lines after the verdict)")
        elif owner:
            absent = [n for n in COUNTS if ev.counts.get(n) is None]
            if absent:
                problems.append("its Coverage line does not give " + ", ".join(absent) +
                                " (all five counts are needed; a missing one is never read as zero)")
            # CONSENT_ATTRIBUTION_OK: meta — states what the mechanism may cover, claims no approval
            # Ori's in-chat approval covers checks the walk could NOT test — never bugs it FOUND (fresh CTO review of
            # #5278, HIGH-1: a full-coverage CHANGES-REQUIRED verdict with an open Critical was posted as APPROVED). The
            # qa-specialist's declared count decides (FW-1); the one-format count is a second reading that only adds.
            declared = ev.open_findings
            if declared is None:
                problems.append("its header has no `Open findings: C<n>/H<n>/M<n>/L<n>` line (after Coverage) — ask the "
                                "qa-specialist to re-issue its verdict with it; never edit the verdict file")
            elif declared["C"] or declared["H"]:
                problems.append(f"it declares open Critical / High findings (C{declared['C']}/H{declared['H']}) — an "
                                f"in-chat approval covers checks that could not be tested, never found bugs: fix them "
                                f"and walk the new head")
            f = ev.findings
            if f.get("C", 0) or f.get("H", 0):
                problems.append(f"it has open Critical / High findings (C{f.get('C', 0)}/H{f.get('H', 0)}) — an in-chat "
                                f"approval covers checks that could not be tested, never found bugs: fix them and walk "
                                f"the new head")
            untested = sum(ev.counts.get(n) or 0 for n in ("blocked-accepted", "rationalized", "missing"))
            untested += max((ev.total or 0) - (ev.walked or 0), 0)
            if (ev.verdict == CHANGES or ev.release != "GO") and untested == 0:
                problems.append("the verdict is held for its findings, not for checks that could not be tested — an "
                                "in-chat approval cannot cover that: fix them and walk the new head")
        else:
            absent = [n for n in COUNTS if ev.counts.get(n) is None]
            if absent:
                problems.append("its Coverage line does not give " + ", ".join(absent) +
                                " (all five counts are needed; a missing one is never read as zero)")
            walked = (ev.counts.get("live") or 0) + (ev.counts.get("test-fidelity") or 0)
            if ev.total == 0 or ev.walked != ev.total or (not absent and walked != ev.total):
                problems.append(f"not every cell was walked ({ev.walked}/{ev.total} cells; live + test-fidelity = "
                                f"{walked}) — walk the rest; never edit the verdict file")
            chat = (f"ask Ori in the chat (python3 scripts/approve-merge.py ask --pr {pr} --sha {head} --evidence "
                    "<this file> --untested \"…\") and after his `approve` run this again with --owner-approved, or "
                    "use his GitHub Mobile override")
            for name, why in (("blocked-accepted", f"a blocked cell is the owner's decision — {chat}"),
                              ("rationalized", f"rationalized, not walked — walk it, or {chat}"),
                              ("missing", f"missing — walk it, or {chat}")):
                if ev.counts.get(name):
                    problems.append(f"{ev.counts[name]} cell(s) {name}: {why}")
    return problems


def marker(verdict: str, pr: int, sha: str, ev: Evidence, report: str | None, owner: bool = False) -> str:
    f = ev.open_findings or ev.findings  # the qa-specialist's own count when it gives one
    walked = f" walked-by={ev.walked_by}" if ev.walked_by else ""
    by_owner = " owner=chat" if owner else ""
    return (f"QA-REVIEW v1 verdict={verdict} pr={pr} sha={sha} cells={ev.walked or 0}/{ev.total or 0} "
            f"findings=C{f.get('C', 0)}/H{f.get('H', 0)}/M{f.get('M', 0)}/L{f.get('L', 0)}{walked}{by_owner} "
            f"report={report or '-'}")


def parse_marker(body: str | None) -> dict | None:
    """The marker on a review's FIRST line, or None. AC4.9's merged-PR audit asks this, never a copy of it. Markers
    posted before merge-queue-simplify carry no walker (`walked_by` None)."""
    first = (body or "").split("\n", 1)[0].strip()
    m = MARKER_RE.match(first)
    if not m:
        return None
    return {"verdict": m["verdict"], "pr": int(m["pr"]), "sha": m["sha"], "report": m["report"],
            "walked_by": m["walked_by"], "owner": m["owner"], "findings": {k: int(m[k]) for k in "CHML"}}


def owner_may_supersede(r: dict, pr: int, head: str) -> bool:
    # CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism; claims no approval
    """Whether Ori's in-chat approval may be posted over this QA review of the head: only a request for changes that
    qa-review.sh posted for THIS PR and commit with no Critical / High finding — the QA runner's CHANGES-REQUIRED for
    checks it could not test, the very case the chat approval is for (QA charter of #5278, FW-2). A dismissal (a
    person's word) or a review without the marker never is."""
    mark = parse_marker(r.get("body"))
    return (r.get("state") == "CHANGES_REQUESTED" and mark is not None and mark["verdict"] == CHANGES
            and mark["pr"] == pr and mark["sha"] == head and not mark["findings"]["C"] and not mark["findings"]["H"])


def builder_walk_refused_files(files: list[str]) -> list[str]:
    """The files whose NAME keeps a builder-driven walk from being posted (AC-B3) — a test file alone never does."""
    return [f for f in files if _CODE_ROOT.search(f) and not _NOT_RISK.search(f) and QA_RUNNER_PATHS.search(f)]


# A change touches money when its CHANGED lines name the payment provider or a money table (the payment-review skill's
# table list), whatever the file is called — names alone missed the in-doubt Takbull resolution and the class-participant
# service (QA charter WG-1, the third review to find name misses). Changed lines, not the whole file: the big shared
# files mention refunds somewhere, and a whole-file rule refused 18 of the last 30 app commits (this one: 6, measured).
MONEY_LINES = re.compile(
    # charge / paid / agorot (QA audit, finding 7: the membership start gate and intro-offer trial prices, real money
    # commits on main, changed only these — measured: +1 refusal over the last 30 app commits, 7 instead of 6)
    r"[Tt]akbull|TAKBULL|\b[Hh]yp[A-Z]\w*|HYP_|[Rr]efund|[Cc]harge|paymentToken|cardToken|[Aa]gorot|\b[Pp]aid\b|isPaid"
    r"|[Mm]arkPaid|[Uu]npaid"  # unpaid: round 2, finding B (\b[Pp]aid\b cannot see it inside the word)
    r"|class_membership_payments|pending_membership_purchases|pending_deposits|class_participant_payments"
    r"|subscription_invoices|platform_payment_tokens|classMembershipPayments|pendingMembershipPurchases"
    r"|pendingDeposits|classParticipantPayments|subscriptionInvoices|platformPaymentTokens")
_TEXT_CODE = re.compile(r"\.(?:ts|tsx|js|jsx|mjs|cjs|sql)$")


def _changed_lines(patch: str) -> str:
    return "\n".join(line[1:] for line in patch.splitlines() if line[:1] in "+-" and not line.startswith(("+++", "---")))


def builder_walk_refused_rows(rows: list) -> list[str]:
    """What keeps a builder-driven walk from being posted, one entry per file (GitHub's /pulls/N/files rows): its name
    or its old name (payments / login / schema), its changed lines touching money, or — fail closed — a code file whose
    changes GitHub does not show (a diff too large for its `patch`)."""
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        by_name = builder_walk_refused_files([str(n) for n in (r.get("filename"), r.get("previous_filename")) if n])
        f = str(r.get("filename") or "")
        if by_name:
            out += by_name
        elif _CODE_ROOT.search(f) and not _NOT_RISK.search(f) and _TEXT_CODE.search(f):
            patch = r.get("patch")
            if not isinstance(patch, str):
                out.append(f"{f} (GitHub does not show its changes)")
            elif MONEY_LINES.search(_changed_lines(patch)):
                out.append(f"{f} (its changed lines touch payments)")
    return out


def review_body(mark: str, evidence_name: str, text: str, report: str | None, owner: dict | None = None) -> str:
    walk = text if len(text) <= BODY_CAP else text[:BODY_CAP] + "\n\n…(the rest of the report is cut here)"
    where = f" The full report: {report}" if report else ""
    # CONSENT_ATTRIBUTION_OK: meta — formats a record of an approval the pass proves, claims none itself
    said = (f"\n\n**OWNER OVERRIDE (approved in chat, {owner.get('approved_at')})** — Ori's reply: «{owner.get('message')}»."
            f"\n**Not tested:** {owner.get('untested') or '(not given)'}" if owner else "")
    return (f"{mark}{said}\n\nPosted by `scripts/qa-review.sh` from the QA verdict `{evidence_name}`.{where} The walk's "
            f"verdict follows.\n\n---\n\n{walk}")


@dataclass
class OwnerApproval:
    # CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism or labels its record; claims no approval
    """Ori's in-chat approval (owner-chat-approve): `load(pr, head, verdict_sha256)` → (pass, why-not);
    `consume(pr, head)` after the review is posted. Built by main() from scripts/owner_approval.py."""
    load: Callable[[int, str, str], tuple]
    consume: Callable[[int, str], None]


# ── GitHub ──────────────────────────────────────────────────────────────────────────────────────────────

@dataclass
class Reply:
    status: int
    data: object
    headers: dict


Api = Callable[[str, str, "dict | None"], Reply]


def make_api(token: str) -> Api:
    def api(method: str, path: str, body: dict | None = None) -> Reply:
        req = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body else None)
        req.add_unredirected_header("Authorization", f"Bearer {token}")  # never re-sent to a redirect's host (QA F4)
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("X-GitHub-Api-Version", "2022-11-28")
        req.add_header("User-Agent", "bookwith-qa-review")
        if body is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=API_TIMEOUT_S) as resp:  # noqa: S310 — fixed https host
                raw = resp.read()
                return Reply(resp.status, json.loads(raw) if raw else None, dict(resp.headers))
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                data = json.loads(raw) if raw else None
            except ValueError:
                data = None
            return Reply(e.code, data, dict(e.headers or {}))
        except Exception as e:  # noqa: BLE001 — ONLY the type name: a message could carry the request's headers
            raise Stop(EXIT_CANNOT_ASK, f"qa-review: GitHub could not be asked ({type(e).__name__}). Check the "
                                        f"network and run it again.") from None
    return api


def _message(reply: Reply) -> str:
    return (reply.data or {}).get("message", "") if isinstance(reply.data, dict) else ""


def _auth_failure(reply: Reply, doing: str) -> None:
    """401 / 403 are the token's own problem, not GitHub's: say so, with how to fix it. A used-up rate limit is
    neither: it says wait (QA of #5011, F5)."""
    remaining = next((v for k, v in reply.headers.items() if k.lower() == "x-ratelimit-remaining"), None)
    if reply.status in (403, 429) and (remaining == "0" or "rate limit" in _message(reply).lower()):
        reset = next((v for k, v in reply.headers.items() if k.lower() == "x-ratelimit-reset"), None)
        when = ""
        if reset and reset.isdigit():
            from datetime import datetime, timezone
            when = f" (it resets at {datetime.fromtimestamp(int(reset), timezone.utc):%H:%M} UTC)"
        raise Stop(EXIT_CANNOT_ASK, f"qa-review: GitHub's rate limit for the QA token is used up while {doing}{when} — "
                                    f"wait and run it again; the token itself is fine.")
    if reply.status == 401:
        raise Stop(EXIT_NO_TOKEN, f"qa-review: GitHub refused the QA token while {doing} (401 — expired or revoked). "
                                  f"{MINT_HELP}")
    if reply.status == 403:
        raise Stop(EXIT_NO_ACCESS, f"qa-review: the QA token may not do this while {doing} (403 {_message(reply)}). "
                                   f"{MINT_HELP}")


def get_all(api: Api, path: str) -> list:
    """Every page of a list endpoint (reviews: up to 100 a page)."""
    out, page = [], 1
    while True:
        r = api("GET", f"{path}{'&' if '?' in path else '?'}per_page=100&page={page}", None)
        _auth_failure(r, f"reading {path.split('?')[0]}")
        if r.status != 200 or not isinstance(r.data, list):
            raise Stop(EXIT_CANNOT_ASK, f"qa-review: GitHub answered {r.status} reading {path.split('?')[0]} "
                                        f"({_message(r)}) — nothing was posted.")
        out += r.data
        if len(r.data) < 100:
            return out
        page += 1


# ── the steps ───────────────────────────────────────────────────────────────────────────────────────────

@dataclass
class Deps:
    api: Api
    out: Callable[[str], None]
    probe: Callable[[str, int, str], tuple]      # (repo, pr, sha) → (verdict, message)
    run_queue: Callable[[str, int, str, bool, "int | None"], int]  # (repo, pr, sha, no_wait, the approval's review id)


def check(repo: str, token: str, deps: Deps) -> int:
    if not token:
        raise Stop(EXIT_NO_TOKEN, f"qa-review: no QA token on this machine (secret store: account github-qa, service "
                                  f"claude-code-mcp). {MINT_HELP}")
    me = deps.api("GET", "/user", None)
    _auth_failure(me, "asking who it is")
    if me.status != 200 or not isinstance(me.data, dict):
        raise Stop(EXIT_CANNOT_ASK, f"qa-review: GitHub answered {me.status} to 'who am I' ({_message(me)}).")
    login = me.data.get("login")
    if login != QA_LOGIN:
        raise Stop(EXIT_WRONG_ACCOUNT, f"qa-review: the stored token belongs to {login!r}, not the QA account "
                                       f"{QA_LOGIN!r} — an approval from it would not count. {MINT_HELP}")
    repo_reply = deps.api("GET", f"/repos/{repo}", None)
    _auth_failure(repo_reply, f"reading {repo}")
    if repo_reply.status != 200 or not isinstance(repo_reply.data, dict):
        raise Stop(EXIT_NO_ACCESS, f"qa-review: the QA token cannot see {repo} ({repo_reply.status} "
                                   f"{_message(repo_reply)}). {MINT_HELP}")
    if not (repo_reply.data.get("permissions") or {}).get("push"):
        raise Stop(EXIT_NO_ACCESS, f"qa-review: the QA account has no Write role on {repo}, so GitHub would not count "
                                   f"its approval — Ori adds the repository to team qa-approvers with Write "
                                   f"(owner-setup.md §3).")
    pulls = deps.api("GET", f"/repos/{repo}/pulls?per_page=1", None)
    _auth_failure(pulls, f"reading the pull requests of {repo}")
    if pulls.status != 200:
        raise Stop(EXIT_NO_ACCESS, f"qa-review: the QA token cannot read the pull requests of {repo} ({pulls.status} "
                                   f"{_message(pulls)}). {MINT_HELP}")
    expiry = next((v for k, v in me.headers.items() if k.lower() == "github-authentication-token-expiration"), None)
    deps.out(f"✓ QA token OK — it is {QA_LOGIN}, with Write on {repo}, and it can read its pull requests."
             + (f" It expires {expiry}." if expiry else "") + " Whether it may WRITE a review is first seen when "
             "one is posted (a refusal exits 5).")
    if expiry:
        days = _days_left(expiry)
        if days is not None and days <= EXPIRY_WARN_DAYS:
            deps.out(f"⚠️  It expires in {days} day(s). {MINT_HELP}")
    return EXIT_OK


def _days_left(expiry: str) -> int | None:
    from datetime import datetime, timezone
    for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S UTC", "%Y-%m-%d"):
        try:
            when = datetime.strptime(expiry.strip(), fmt).replace(tzinfo=timezone.utc)
            return (when - datetime.now(timezone.utc)).days
        except ValueError:
            continue
    return None


def _queue_result(rc: int, repo: str, pr: int, head: str) -> int:
    if rc == EXIT_QUEUED:
        return EXIT_QUEUED
    _say(f"qa-review: the approval IS posted on #{pr}, but queueing did not finish (the queue command exited {rc}; "
         f"its own words are above). Run `python3 scripts/queue-pr.py --pr {pr} --sha {head} --repo {repo}` when the "
         f"reason is gone — it never queues twice. Do NOT post the approval again.")
    return EXIT_POSTED_NOT_QUEUED


def review(repo: str, pr: int, want: str, sha_arg: str, evidence_path: str, token: str, deps: Deps,
           no_wait: bool = False, report: str | None = None, owner: OwnerApproval | None = None) -> int:
    if not token:
        raise Stop(EXIT_NO_TOKEN, f"qa-review: no QA token on this machine — nothing was posted. {MINT_HELP}")
    if not re.fullmatch(r"[0-9a-f]{7,40}", sha_arg):
        raise Stop(EXIT_USAGE, f"qa-review: --sha must be the walked commit's id (7 to 40 hex characters), got {sha_arg!r}")
    try:
        with open(evidence_path, encoding="utf-8") as fh:
            text = fh.read()
    except OSError as e:
        raise Stop(EXIT_REFUSED, f"qa-review: cannot read the evidence file {evidence_path} ({e.strerror}) — nothing "
                                 f"was posted.") from None
    except UnicodeDecodeError:
        raise Stop(EXIT_REFUSED, f"qa-review: the evidence file {evidence_path} is not UTF-8 text — nothing was "
                                 f"posted.") from None

    pull = deps.api("GET", f"/repos/{repo}/pulls/{pr}", None)
    _auth_failure(pull, f"reading #{pr}")
    if pull.status == 404:
        raise Stop(EXIT_USAGE, f"qa-review: there is no PR #{pr} in {repo} that the QA token can see.")
    if pull.status != 200 or not isinstance(pull.data, dict):
        raise Stop(EXIT_CANNOT_ASK, f"qa-review: GitHub answered {pull.status} reading #{pr} ({_message(pull)}).")
    p = pull.data
    head = (p.get("head") or {}).get("sha") or ""
    if p.get("state") != "open" or p.get("merged_at"):
        raise Stop(EXIT_REFUSED, f"qa-review: #{pr} is {'merged' if p.get('merged_at') else p.get('state')} — "
                                 f"nothing was posted.")
    if p.get("draft"):
        raise Stop(EXIT_REFUSED, f"qa-review: #{pr} is a draft — mark it ready first. Nothing was posted.")
    if not head.startswith(sha_arg):
        raise Stop(EXIT_REFUSED, f"qa-review: #{pr}'s head is {head[:9]}, not the walked commit {sha_arg[:9]} — the "
                                 f"verdict is about a commit that is no longer what would merge. Walk the new head. (A "
                                 f"rebase that keeps the PR's own diff identical keeps an existing QA approval by itself "
                                 f"— AC5.0a test 2 — so check #{pr}'s reviews first.) Nothing was posted.")

    owner_pass = None
    if owner is not None:
        with open(evidence_path, "rb") as fh:
            digest = hashlib.sha256(fh.read()).hexdigest()
        owner_pass, why = owner.load(pr, head, digest)
        if owner_pass is None:
            raise Stop(EXIT_REFUSED, f"qa-review: --owner-approved, but {why}. Nothing was posted.")
    ev = parse_evidence(text)
    what = "an approval" if want == APPROVED else "a request for changes"
    problems = evidence_problems(ev, want, pr, head, owner=owner_pass is not None)
    if owner_pass is not None:
        problems += owner_findings_problems(text)
    if problems:
        raise Stop(EXIT_REFUSED, f"qa-review: the evidence {evidence_path} cannot back {what} of #{pr} at "
                                 f"{head[:9]}: " + "; ".join(problems) + ". Nothing was posted.")
    if want == APPROVED and ev.walked_by == "builder" and owner_pass is None:
        # get_all raises on an unreadable page: files that cannot be read never let a builder walk through
        rows = get_all(deps.api, f"/repos/{repo}/pulls/{pr}/files")
        refused = builder_walk_refused_rows(rows)
        if refused:
            shown = ", ".join(refused[:6]) + (f" (+{len(refused) - 6} more)" if len(refused) > 6 else "")
            # a schema PR is never armed (queue-pr.py, the probe's own rule: shared/schema/ minus markdown)
            schema = any(str(r.get("filename") or "").startswith("shared/schema/") and
                         not str(r.get("filename") or "").endswith(".md") for r in rows if isinstance(r, dict))
            after = (f"a schema PR is never armed, so after that approval a session runs python3 scripts/queue-pr.py "
                     f"--pr {pr} (prod must already have the columns)" if schema else
                     f"where \"Allow auto-merge\" is on, arm it first (python3 scripts/queue-pr.py --pr {pr}) and it "
                     f"queues on that approval; otherwise run that command after it")
            raise Stop(EXIT_REFUSED, f"qa-review: #{pr} changes payments, login or the database schema ({shown}), and "
                                     f"the builder drove this walk — such a PR needs the QA runner's (or a fresh QA "
                                     # CONSENT_ATTRIBUTION_OK: meta — names the override mechanisms, claims no approval
                                     f"agent's) own walk, or Ori's decision: ask him in the chat (python3 "
                                     f"scripts/approve-merge.py ask --pr {pr} --sha {head} --evidence <this file> "
                                     f"--untested \"an independent walk of the payments / login / schema change\") and "
                                     f"after his `approve` run this again with --owner-approved; or his approval in "
                                     f"GitHub Mobile as bookwith-qa with a review starting OWNER OVERRIDE ({after}). "
                                     f"Nothing was posted.")

    mine = [r for r in get_all(deps.api, f"/repos/{repo}/pulls/{pr}/reviews")
            if (r.get("user") or {}).get("login") == QA_LOGIN and r.get("commit_id") == head]
    if want == APPROVED:
        blocking = [r for r in mine if r.get("state") in ("CHANGES_REQUESTED", "DISMISSED")
                    and not (owner_pass is not None and owner_may_supersede(r, pr, head))]
        if blocking:
            r = blocking[0]
            why = ("asked for changes on it" if r.get("state") == "CHANGES_REQUESTED" else
                   "has a review on it that a PERSON dismissed (GitHub dismisses only on a real change, and that "
                   "moves the head)")
            raise Stop(EXIT_REFUSED, f"qa-review: the QA account {why} (review {r.get('id')}) — the first verdict on a "
                                     f"commit stands, and this command does not talk over a dismissal. The builder "
                                     f"pushes a fix and the new head is walked, or ask whoever dismissed it (or Ori). "
                                     f"Nothing was posted.")
    else:
        approved = [r for r in mine if r.get("state") == "APPROVED"]
        if approved:
            raise Stop(EXIT_REFUSED, f"qa-review: the QA account already approved {head[:9]} (review "
                                     f"{approved[0].get('id')}) — the first verdict on a commit stands. If that "
                                     f"approval was wrong, tell Ori: the PR may already be queued, and he can take it "
                                     f"out on its page. Nothing was posted.")

    if want == APPROVED:
        verdict, message = deps.probe(repo, pr, head)
        deps.out(f"  {message}")
        if verdict == "refuse":
            raise Stop(EXIT_REFUSED, f"qa-review: the database-schema step said no for {head[:9]} (above) — no "
                                     f"approval was posted.")

    gh_state = "APPROVED" if want == APPROVED else "CHANGES_REQUESTED"
    already = [r for r in mine if r.get("state") == gh_state and
               (parse_marker(r.get("body")) or {}).get("sha") == head]
    if already:
        review_id = already[0].get("id")
        deps.out(f"  The QA account's {want} review of {head[:9]} from this command is already on #{pr} (review "
                 f"{review_id}) — not posting a second one.")
    else:
        mark = marker(want, pr, head, ev, report, owner=owner_pass is not None)
        event = "APPROVE" if want == APPROVED else "REQUEST_CHANGES"
        posted = deps.api("POST", f"/repos/{repo}/pulls/{pr}/reviews",
                          {"commit_id": head, "event": event,
                           "body": review_body(mark, os.path.basename(evidence_path), text, report, owner_pass)})
        _auth_failure(posted, f"posting {what} on #{pr}")
        data = posted.data if isinstance(posted.data, dict) else {}
        if posted.status != 200 or data.get("state") != gh_state or data.get("commit_id") != head:
            moved = (" GitHub refuses a review pinned to a commit that stopped being the head while this ran — if "
                     f"the PR's head moved, the new head needs its own walk." if posted.status == 422 else "")
            raise Stop(EXIT_CANNOT_ASK, f"qa-review: GitHub did not record the review as asked ({posted.status} "
                                        f"{_message(posted) or data.get('state')}).{moved} Check #{pr}'s reviews before "
                                        f"running it again.")
        review_id = data.get("id")
        deps.out(f"✓ posted the QA account's {want} review of #{pr}, pinned to {head[:9]} (review {review_id}).")
        deps.out(f"  marker: {mark}")
    if owner is not None and owner_pass is not None:
        owner.consume(pr, head)  # one approval, one use — also when the review was already there
    if want != APPROVED:
        return EXIT_OK
    deps.out(f"  queueing #{pr} at {head[:9]}:")
    # From here the review IS posted: a stop (Ctrl-C, or the SIGTERM an agent's tool timeout sends) must say so, not end
    # on the queue command's own "nothing was queued" (QA of #5011, F2). Said the moment the stop arrives — a harness
    # that follows its SIGTERM with a SIGKILL inside the queue command's grace would otherwise leave no line at all
    # (QA of #5015, Q2) — and said again last, after the queue command's own words.
    line = (f"qa-review: stopped while queueing — the approval IS posted on #{pr} at {head[:9]}; whether GitHub queued "
            f"the PR is not known here. Ask `python3 scripts/queue-pr.py --status {pr} --repo {repo}`, and if it is not "
            f"queued run `python3 scripts/queue-pr.py --pr {pr} --sha {head} --repo {repo}` — it never queues twice. "
            f"Do NOT post the approval again.")
    # A signal the caller ignores stays ignored, here and in the queue command it starts: `nohup … &` runs with SIGINT
    # and SIGHUP ignored, and a stray Ctrl-C to the group must not stop that run (queue-pr.py keeps the same rule).
    stops = [sig for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP) if signal.getsignal(sig) is not signal.SIG_IGN]
    _STOP.clear()

    def stopped(signum, _frame):
        if not _STOP:  # the first stop counts; another changes nothing, so the queue command's grace is never cut short
            _STOP.append(signum)
            if _CHILD:
                try:
                    os.kill(_CHILD[0], signum)
                except OSError:
                    pass
            _say(line)
    # The handlers go in INSIDE the try, and come out only after the last line is said: a Ctrl-C landing between two
    # installs (Python's own handler raises KeyboardInterrupt there) is a stop like any other (cto review of
    # 59f0c9a4ed, L1). A stop landing while they are restored can still end the run in a traceback (Python's own
    # handler is back by then), but only after the honest line was said (QA of 6d48b9ba55, F5).
    previous: dict = {}
    try:
        for sig in stops:
            previous[sig] = signal.signal(sig, stopped)
        try:
            # the approval's id: only while THAT review is unreadable does the queue command leave a lagging read to
            # GitHub (fresh CTO delta review of #5106, M1); an id GitHub did not give is passed as None
            rc = deps.run_queue(repo, pr, head, no_wait, review_id if str(review_id).isdigit() else None)
        except (Interrupted, KeyboardInterrupt):
            rc = None
        if _STOP and rc == EXIT_QUEUED:
            _say(f"qa-review: the stop came as the queue command finished — #{pr} IS queued at {head[:9]} (queued, not "
                 f"merged). Nothing more to run; do NOT post the approval again.")
            return EXIT_QUEUED
        if _STOP or rc is None:
            _say(line)
            return EXIT_POSTED_NOT_QUEUED
        return _queue_result(rc, repo, pr, head)
    except KeyboardInterrupt:  # a Ctrl-C while the handlers were going in, before the queue command ever started
        _say(line)
        return EXIT_POSTED_NOT_QUEUED
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
        _STOP.clear()


# ── wiring ──────────────────────────────────────────────────────────────────────────────────────────────

_QUEUE_PR = None


def _queue_pr():
    """queue-pr.py, loaded as a module (hyphenated name): its PR parser, repository reader and the merge line's
    schema decision are asked, never copied (CLAUDE.md rule 15)."""
    global _QUEUE_PR
    if _QUEUE_PR is None:
        spec = importlib.util.spec_from_file_location("_queue_pr_for_qa_review", os.path.join(HERE, "queue-pr.py"))
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod  # its @dataclass looks its own module up while the class is built (3.9)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        _QUEUE_PR = mod
    return _QUEUE_PR


def real_owner_approval() -> OwnerApproval:
    # CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism or labels its record; claims no approval
    """Ori's in-chat approval, read from the main checkout's store (scripts/owner_approval.py)."""
    spec = importlib.util.spec_from_file_location("owner_approval", os.path.join(HERE, "owner_approval.py"))
    oa = importlib.util.module_from_spec(spec)
    sys.modules["owner_approval"] = oa
    spec.loader.exec_module(oa)
    root = oa.main_checkout(HOME_ROOT)
    return OwnerApproval(load=lambda pr, head, digest: oa.find_pass(root, pr, head, digest, oa.now_utc()),
                         consume=lambda pr, head: oa.consume_pass(root, pr, head))


def real_probe(repo: str, pr: int, sha: str, qp=None) -> tuple:
    qp = qp or _queue_pr()
    deps = qp.real_deps()
    home_repo, home_root = deps.home()
    if not home_repo:
        return "refuse", ("prod-schema probe: could not ask GitHub which repository this checkout is, so it cannot "
                          "tell whether the database-schema check applies")
    if not qp.same_repo(home_repo, repo):
        return "n/a", f"prod-schema probe: #{pr} is in {repo}, not {home_repo} — no probe applies."
    if not deps.ensure_commit(home_root, pr, sha):
        return "refuse", (f"prod-schema probe: commit {sha[:9]} is not in this checkout ({home_root}) — run "
                          f"`git fetch origin pull/{pr}/head` there, then run this again")
    verdict, message, _owed = deps.probe_decide(home_root, str(pr), sha, "qa-approve")
    return verdict, message


def real_run_queue(repo: str, pr: int, sha: str, no_wait: bool, review_id: "int | None" = None) -> int:
    """The queue command's exit code; raises Interrupted when a stop (stop_requested) ended it. A stop is passed on as
    the very signal received, then the command gets QUEUE_STOP_GRACE_S to take its wait label off (its own handlers
    do that) before it is killed. If it had queued the PR before the stop reached it, that is its answer: 10."""
    if stop_requested():
        raise Interrupted("stopped before the queue command started")
    env = {k: v for k, v in os.environ.items() if k != TOKEN_ENV}  # belt and braces: main() popped it already
    cmd = [sys.executable, os.path.join(HERE, "queue-pr.py"), "--pr", str(pr), "--sha", sha, "--repo", repo]
    if review_id is not None:  # queue-pr.py's hidden flag: the approval this command just posted (or found) on `sha`
        cmd += ["--approved-review", str(review_id)]
    if no_wait:
        cmd.append("--no-wait")
    proc = subprocess.Popen(cmd, env=env)
    _CHILD[:] = [proc.pid]
    try:
        while True:
            try:
                rc = proc.wait(timeout=STOP_POLL_S)
            except subprocess.TimeoutExpired:
                rc = None
            sig = stop_requested()
            if sig is None:
                if rc is not None:
                    return rc
                continue
            if rc is None:
                # Again, for a stop the handler saw before the pid was known. When it already reached the command,
                # this second one is ignored: queue-pr.py's `_stopped` ignores every stop after the first.
                try:
                    proc.send_signal(sig)
                except OSError:
                    pass
                try:
                    rc = proc.wait(timeout=QUEUE_STOP_GRACE_S)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
            if rc == EXIT_QUEUED:
                return rc
            raise Interrupted(signal.Signals(sig).name)
    finally:
        _CHILD.clear()


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="qa-review.sh", description=__doc__.split("\n\n")[0],
                                 epilog="EXIT CODES\n" + __doc__.split("EXIT CODES", 1)[1].strip(),
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("pr", nargs="?", help="the pull request: 7, #7 or its URL")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="the token is there, is the QA account, and can act on the repo")
    mode.add_argument("--approve", action="store_true", help="post the QA account's approval, then queue the PR")
    mode.add_argument("--request-changes", action="store_true", help="post the QA account's request for changes")
    ap.add_argument("--sha", help="the commit the walk was on")
    ap.add_argument("--evidence", help="the qa-specialist's verdict file for that walk")
    ap.add_argument("--report-url", help="a link to the walk's full report (named in the review and its marker)")
    ap.add_argument("--repo", help="owner/name (default: the repository this script belongs to)")
    ap.add_argument("--no-wait", action="store_true", help="passed to the queue command: do not wait for running checks")
    ap.add_argument("--owner-approved", action="store_true",
                    # CONSENT_ATTRIBUTION_OK: meta — describes the in-chat approval mechanism or labels its record; claims no approval
                    help="Ori approved this verdict in the chat (scripts/approve-merge.py confirm made the pass)")
    a = ap.parse_args(argv)
    if a.owner_approved and not a.approve:
        ap.error("--owner-approved goes with --approve")
    if (a.approve or a.request_changes) and not (a.pr and a.sha and a.evidence):
        ap.error("--approve and --request-changes need <pr>, --sha and --evidence")
    if a.check and (a.pr or a.sha or a.evidence):
        ap.error("--check takes only --repo")
    if a.report_url and not re.fullmatch(r"https://\S+", a.report_url):
        ap.error("--report-url must be one https:// link without spaces")
    return a


def read_token(from_env: str) -> "tuple[str, str, bool]":
    """(token, why nothing was read, whether the STORED token itself is the problem). The token comes from
    qa-review.sh's pipe on the descriptor it names, closed once read; else from what a direct run put in the
    environment (already popped). A descriptor is read ONLY when qa-review.sh named it — any other fd 3 a parent left
    open is not ours to read. "Why" is set only when nothing usable came through, so a failed handover is never
    mistaken for a missing token (fresh cto review of f20ecaa090, L6) — and a token that DID come through but is longer
    than any GitHub token, or not text, is the stored token's fault: mint a new one (QA of 59f0c9a4ed, F2)."""
    fd = os.environ.pop(TOKEN_FD_ENV, "")
    if fd != "3":
        return from_env, "", False
    # Only a pipe is the wrapper's. A direct run that names fd 3 with it closed lets Python's own /dev/urandom take
    # that number, and an unbounded read of it never ends (QA of #5015, Q1) — so: a pipe, read to at most
    # TOKEN_MAX_BYTES and for at most TOKEN_READ_S (a pipe whose writer never closes ends the read too).
    try:
        if not stat.S_ISFIFO(os.fstat(3).st_mode):
            return "", "file descriptor 3 is not the wrapper's pipe", False
    except OSError:
        return "", "file descriptor 3 is not open", False
    raw = b""
    deadline = time.monotonic() + TOKEN_READ_S
    try:
        while True:
            left = deadline - time.monotonic()
            if left <= 0 or not select.select([3], [], [], left)[0]:
                return "", f"the token's pipe did not close within {TOKEN_READ_S} s", False
            chunk = os.read(3, 4096)
            if not chunk:
                break
            raw += chunk
            if len(raw) > TOKEN_MAX_BYTES:
                return "", f"it is longer than any GitHub token (over {TOKEN_MAX_BYTES} bytes)", True
    except OSError as e:
        return "", f"the token's pipe could not be read ({type(e).__name__})", False
    finally:
        try:
            os.close(3)
        except OSError:
            pass
    try:
        return raw.decode("utf-8"), "", False
    except UnicodeDecodeError:
        return "", "it holds bytes that are not UTF-8 text", True


def main(argv: list[str] | None = None, deps: Deps | None = None, token: str | None = None,
         owner: OwnerApproval | None = None) -> int:
    # FIRST: the token leaves the environment, so no child — gh, git, the prod-schema probe running the PR's own
    # schema code, the queue command — can ever see it (fresh cto review of #5011, M1).
    from_env = os.environ.pop(TOKEN_ENV, "")
    token, handover, stored_is_bad = read_token(from_env) if token is None else (token, "", False)
    try:
        a = parse_args(sys.argv[1:] if argv is None else argv)
    except SystemExit as e:
        return EXIT_USAGE if e.code else EXIT_OK
    try:
        if handover and stored_is_bad:
            raise Stop(EXIT_NO_TOKEN, f"qa-review: the stored QA token is unusable — {handover}. It was not sent "
                                      f"anywhere. {MINT_HELP}")
        if handover:
            raise Stop(EXIT_NO_TOKEN, f"qa-review: the QA token was not handed over — {handover}. Nothing was sent "
                                      f"anywhere. Run it as `bash scripts/qa-review.sh …`; the stored token itself may "
                                      f"be fine.")
        if token and not _TOKEN_SHAPE.fullmatch(token):
            raise Stop(EXIT_NO_TOKEN, "qa-review: the stored QA token has characters a GitHub token never has (a stray "
                                      f"space or line break?) — it was not sent anywhere. {MINT_HELP}")
        qp = _queue_pr()
        repo = a.repo or qp._github_repo_of(HOME_ROOT)
        if not repo:
            raise Stop(EXIT_CANNOT_ASK, "qa-review: could not tell which repository this checkout is — pass --repo owner/name.")
        deps = deps or Deps(api=make_api(token), out=lambda s: print(s, flush=True), probe=real_probe,
                            run_queue=real_run_queue)
        if a.check:
            return check(repo, token, deps)
        try:
            number, url_repo = qp.parse_pr(a.pr)
        except ValueError as e:
            raise Stop(EXIT_USAGE, f"qa-review: {e}") from None
        if url_repo and not qp.same_repo(url_repo, repo):
            raise Stop(EXIT_USAGE, f"qa-review: the PR URL is in {url_repo}, but --repo is {repo}.")
        if a.owner_approved and owner is None:
            owner = real_owner_approval()
        return review(repo, number, APPROVED if a.approve else CHANGES, a.sha.lower(), a.evidence, token, deps,
                      a.no_wait, a.report_url, owner if a.owner_approved else None)
    except Stop as s:
        _say(str(s))
        return s.code


def _exit(rc: int) -> None:
    """sys.exit(rc), whatever became of stderr. A line still held for a stderr that is full (a non-blocking pipe
    nobody reads) or gone makes Python's own flush at exit fail and turns rc into 120 (QA of 6d48b9ba55, F2): it is
    flushed here, and if that fails, stderr is swapped for /dev/null as _say does."""
    try:
        if sys.stderr is not None:  # None: Python started with fd 2 closed (cto review of 3b3a76e056, L2)
            sys.stderr.flush()
    except (OSError, ValueError, RuntimeError):
        try:
            sys.stderr = open(os.devnull, "w")
        except OSError:
            pass
    sys.exit(rc)


if __name__ == "__main__":
    _exit(main())
