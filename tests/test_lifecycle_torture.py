"""Multi-process, model-checked lifecycle torture test for the core queue.

Separate OS processes drive the raw loadable extension through stdlib
``sqlite3`` on one file-backed WAL database with the real clock. They
enqueue, claim, ack, retry, fail, heartbeat, stall past the lease and
cancel at random. Some share a worker id; a coordinator SIGKILLs some
mid-run and restarts them with the same id. Every process writes a
ledger. After a quiesce and drain, ``lifecycle_torture.check`` checks
the invariants in ``INVARIANTS`` against the ledgers and the final
database. See the module docstring of ``tests/lifecycle_torture.py``.

Run it: ``python -m pytest -o addopts="" -n 0 tests/test_lifecycle_torture.py -rxX``
(it is marked ``slow``, so the default run deselects it).

Knobs (environment):

* ``HONKER_TORTURE_SECONDS``: run length per seed. Default 25 (PR CI).
  The nightly run sets a few minutes.
* ``HONKER_TORTURE_SEEDS``: comma-separated seeds. Default ``1``.
* ``HONKER_TORTURE_PROCS``: worker processes. Default 6.
* ``HONKER_TORTURE_SHARED_IDS``: ``0`` gives each live process its own
  worker id. Default ``1``: two pairs of live processes share an id,
  which models a restarted worker whose old incarnation is still
  finishing a stalled handler.
* ``HONKER_EXTENSION_PATH``: the extension to load. Default
  ``target/release/libhonker_ext.{dylib,so}``.

This test fails, rather than skips, when the extension is missing or
the interpreter's sqlite3 cannot load extensions. It only skips on
Windows, which has no SIGKILL.

Two invariants fail on main today and are marked xfail with their
issues: fencing (2, same worker id stale ack) and stuck expired rows
(5, an in-flight job that expires is never swept). Remove a marker when
its fix lands; the test will then guard the fix.
"""

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lifecycle_torture as lt  # noqa: E402

SECONDS = float(os.environ.get("HONKER_TORTURE_SECONDS", "25"))
SEEDS = [int(s) for s in os.environ.get("HONKER_TORTURE_SEEDS", "1").split(",") if s.strip()]
PROCS = int(os.environ.get("HONKER_TORTURE_PROCS", "6"))
# 0 gives every live process its own worker id (restarts still reuse
# the killed process's id). Used to isolate mutations from the known
# same-id fencing bug.
SHARED_IDS = os.environ.get("HONKER_TORTURE_SHARED_IDS", "1") != "0"

FENCING_ISSUE = "https://github.com/russellromney/honker/issues/176"
STUCK_EXPIRED_ISSUE = "https://github.com/russellromney/honker/issues/177"

KNOWN_BUGS = {
    "2_fencing": (
        "main: ack/retry/fail/heartbeat check worker_id + lease, not the attempt, so a "
        "stale handler with the same worker id acts on the new attempt. " + FENCING_ISSUE
    ),
    "5_expiry": (
        "main: sweep_expired ignores processing rows and claim skips expired rows, so an "
        "in-flight job that expires stays live forever. " + STUCK_EXPIRED_ISSUE
    ),
}

pytestmark = [
    # ~30 s per seed, so it is kept out of the default `-n auto` run and
    # run on its own (CI: a dedicated step with `-n 0`). Under xdist,
    # each worker would start its own torture run.
    pytest.mark.slow,
    pytest.mark.skipif(
        sys.platform == "win32", reason="the torture coordinator needs SIGKILL (POSIX only)"
    ),
]

_RUNS = {}


def _require_extension():
    ext = lt.find_extension()
    if ext is None:
        pytest.fail(
            "honker extension not found (set HONKER_EXTENSION_PATH or run "
            "`cargo build -p honker-extension --release`). The torture test does not skip.",
            pytrace=False,
        )
    if not hasattr(sqlite3.connect(":memory:"), "enable_load_extension"):
        pytest.fail(
            f"{sys.executable}'s sqlite3 cannot load extensions; use an interpreter built "
            "with SQLITE_ENABLE_LOAD_EXTENSION (e.g. uv-managed Python). The torture test does not skip.",
            pytrace=False,
        )
    return ext


@pytest.fixture(scope="module", params=SEEDS, ids=lambda s: f"seed{s}")
def report(request, tmp_path_factory):
    seed = request.param
    if seed not in _RUNS:
        ext = _require_extension()
        workdir = str(tmp_path_factory.mktemp(f"torture-seed{seed}"))
        res = lt.run_torture(workdir, ext, seed, SECONDS, PROCS, SHARED_IDS)
        rep = lt.check(res)
        print(f"\n[torture seed={seed} seconds={SECONDS}] workdir={workdir}\n{rep.stats}")
        _RUNS[seed] = rep
    return _RUNS[seed]


def _invariant_params():
    for inv in lt.INVARIANTS:
        marks = []
        if inv in KNOWN_BUGS:
            marks.append(pytest.mark.xfail(reason=KNOWN_BUGS[inv], raises=AssertionError, strict=False))
        yield pytest.param(inv, marks=marks, id=inv)


def test_run_exercised_the_lifecycle(report):
    """Guard against a run that silently did nothing interesting."""
    s = report.stats
    ops = s["lifecycle_by_op"]
    problems = []
    if s["claims"] < 50:
        problems.append(f"only {s['claims']} claims")
    for op in ("ack", "retry", "fail", "heartbeat", "cancel"):
        if ops[op]["ok"] == 0:
            problems.append(f"no successful {op}")
        if op != "cancel" and ops[op]["miss"] == 0 and SECONDS >= 20:
            problems.append(f"no stale-owner {op} miss")
    if s["kills"] == 0 and SECONDS >= 10:
        problems.append("no SIGKILL happened")
    assert not problems, f"seed={report.seed}: weak run: {problems}; stats={s}"


@pytest.mark.parametrize("invariant", list(_invariant_params()))
def test_invariant(report, invariant):
    assert not report.by_invariant(invariant), report.describe(invariant)

