#!/usr/bin/env python3
"""queue-dropped-notice.py — a PR left GitHub's merge queue without merging: say so on the PR, to Ori (native-merge-queue
SPEC AC4.7).

WHY. GitHub takes a PR out of the queue on its own: a conflict with the PRs ahead of it (the commonest — sandbox row 7),
a red queue run, or a person. Nobody is told; the PR just sits there, and a session that queued it may be long gone.
This posts the news where it happened, and labels the PR `queue-dropped` so a session's start-up sweep lists it
(AC4.6) and nobody has to carry the message.

WHAT IT SAYS is the queue command's own answer: it runs `python3 scripts/queue-pr.py --status <N> --repo <R>`, whose
DROPPED line gives GitHub's reason, the branch and the next step in plain words (queue-pr.py's `_after_drop`) — never a
second copy here. `--status` already waits out the second in which a landing PR reads as "dropped, reason merged"
(sandbox row 28); an UNKNOWN answer is asked again (4 times, 30 s apart) before it is posted as unknown.

  dequeued → MERGED (the dequeue WAS the merge) · QUEUED (back in already) · CLOSED → nothing to say.
             Anything else (DROPPED, NOT QUEUED, UNKNOWN) → one comment that @-mentions Ori, and the label.
  enqueued → the `queue-dropped` label comes off: it marks a PR that is OUT of the queue.

Run by .github/workflows/queue-dropped.yml on `pull_request_target` — which, unlike `pull_request`, runs for a PR
with a merge conflict — from the BASE branch's own scripts; the PR's code is never checked out.

EXIT 0 when it said what was owed (or nothing was); 1 when the comment could not be posted, so the run goes red.
A label that cannot be set is a warning: the comment is the notice.
Env: GITHUB_REPOSITORY, PR_NUMBER, BASE_REF, ACTION (dequeued | enqueued), GH_TOKEN.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
LABEL = "queue-dropped"
LABEL_COLOR, LABEL_DESC = "D93F0B", "Left GitHub's merge queue without merging — see the PR's last notice"
OWNER = "@Oriel00"
STATUS_MERGED, EXIT_QUEUED, EXIT_CANNOT_ASK = 0, 10, 12  # queue-pr.py --status codes (its EXIT TABLE)
UNKNOWN_TRIES, UNKNOWN_WAIT_S = 4, 30
STATUS_TIMEOUT_S = 180


def gh(args: list[str]) -> tuple[int, str, str]:
    try:
        r = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        return 1, "", str(e)
    return r.returncode, r.stdout, r.stderr


def queue_status(repo: str, number: int) -> tuple[int, list[str]]:
    """The queue command's --status answer: (its exit code, its lines)."""
    try:
        r = subprocess.run([sys.executable, os.path.join(HERE, "queue-pr.py"), "--status", str(number), "--repo", repo],
                           capture_output=True, text=True, timeout=STATUS_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return EXIT_CANNOT_ASK, [f"UNKNOWN #{number} — the queue command did not answer within {STATUS_TIMEOUT_S} s."]
    lines = [ln.rstrip() for ln in r.stdout.splitlines() if ln.strip()]
    return r.returncode, lines or [f"UNKNOWN #{number} — the queue command printed nothing (exit {r.returncode}): "
                                   f"{r.stderr.strip()[-200:]}"]


def quiet_reason(code: int, lines: list[str]) -> str | None:
    """Why nothing is owed, or None when the notice is."""
    first = lines[0].split(" ", 1)[0] if lines else ""
    if code == STATUS_MERGED:
        return "it merged — the dequeue was the merge"
    if code == EXIT_QUEUED:
        return "it is back in the queue"
    if first == "CLOSED":
        return "it was closed"
    return None


def notice(number: int, base: str, lines: list[str]) -> str:
    # In a code block, longer than any run of backticks inside: the lines carry the PR's branch name, which its author
    # chose — as plain Markdown, a branch named `@someone` would ping that person, and a fence in it would break out.
    said = "\n".join(lines)
    fence = "`" * max(3, _longest_backtick_run(said) + 1)
    return (f"{OWNER} — #{number} left GitHub's merge queue for `{base}` without merging. What the queue command says "
            f"now (`python3 scripts/queue-pr.py --status {number}`):\n\n{fence}text\n{said}\n{fence}\n\nNothing on the "
            f"branch was changed. The `{LABEL}` label stays on this PR until it is queued again, so a session's start-up "
            f"sweep lists it.")


def _longest_backtick_run(text: str) -> int:
    longest = run = 0
    for ch in text:
        run = run + 1 if ch == "`" else 0
        longest = max(longest, run)
    return longest


def dequeued(repo: str, number: int, base: str, gh_call, status_call, sleep) -> int:
    code, lines = status_call(repo, number)
    for _ in range(UNKNOWN_TRIES - 1):
        if code != EXIT_CANNOT_ASK:
            break
        sleep(UNKNOWN_WAIT_S)
        code, lines = status_call(repo, number)
    quiet = quiet_reason(code, lines)
    print("queue-pr.py --status said:\n  " + "\n  ".join(lines))
    if quiet:
        print(f"queue-dropped-notice: nothing to say on #{number} — {quiet}.")
        return 0
    rc, _out, err = gh_call(["pr", "comment", str(number), "-R", repo, "--body", notice(number, base, lines)])
    if rc != 0:
        print(f"::error title=queue-dropped notice::could not comment on #{number}: {_one_line(err)}")
        return 1
    gh_call(["label", "create", LABEL, "-R", repo, "--color", LABEL_COLOR, "--description", LABEL_DESC, "--force"])
    rc, _out, err = gh_call(["api", "-X", "POST", f"repos/{repo}/issues/{number}/labels", "-f", f"labels[]={LABEL}"])
    if rc != 0:
        print(f"::warning title=queue-dropped notice::commented on #{number}, but could not add the `{LABEL}` label: "
              f"{_one_line(err)}")
    print(f"queue-dropped-notice: told {OWNER} on #{number}.")
    return 0


def enqueued(repo: str, number: int, gh_call) -> int:
    rc, _out, err = gh_call(["api", "-X", "DELETE", f"repos/{repo}/issues/{number}/labels/{LABEL}"])
    if rc != 0 and "Label does not exist" not in err and "Not Found" not in err:
        print(f"::warning title=queue-dropped notice::#{number} is queued again, but the `{LABEL}` label could not be "
              f"taken off: {_one_line(err)}")
    return 0


def _one_line(text: str) -> str:
    return " ".join((text or "").split())[:200].replace("%", "%25")


def main(env: dict[str, str] | None = None, gh_call=gh, status_call=queue_status, sleep=time.sleep) -> int:
    env = dict(os.environ) if env is None else env
    repo, base, action = env.get("GITHUB_REPOSITORY", ""), env.get("BASE_REF", ""), env.get("ACTION", "")
    try:
        number = int(env.get("PR_NUMBER", ""))
    except ValueError:
        number = 0
    if not (repo and number and base) or action not in ("dequeued", "enqueued"):
        print("::error title=queue-dropped notice::GITHUB_REPOSITORY / PR_NUMBER / BASE_REF / ACTION missing or wrong")
        return 1
    if action == "enqueued":
        return enqueued(repo, number, gh_call)
    return dequeued(repo, number, base, gh_call, status_call, sleep)


if __name__ == "__main__":
    sys.exit(main())
