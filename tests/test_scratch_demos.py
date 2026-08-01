"""The scratch demos have to be runnable both documented ways.

    python scratch/scratch_audit.py        # sys.path[0] is scratch/
    python -m scratch.scratch_audit        # sys.path[0] is the repo root

Every demo opens with `from log_demo import start_demo_run`, and under the
second form that raised `ModuleNotFoundError: No module named 'log_demo'` — the
repo root is on the path, `scratch/` is not — so the demo died on line one,
before it did anything worth demonstrating. `scratch/__init__.py` puts both
directories on `sys.path`, and `python -m` imports the package before it runs
the module, so the inserts happen first.

The important tests here are subprocesses. The failure is a property of *how
the interpreter was started* — what leads `sys.path` — and the test process
cannot reproduce it, because pytest runs from the repo root with `scratch/`
reachable through neither entry. That is the Phase 5.6 lesson about
`python -m pipeline.nightly` again: an import-based assertion passes on exactly
the code path that is broken.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent
SCRATCH_DIR = PROJECT_ROOT / "scratch"

# `check_db.py` is excluded deliberately: it is a utility rather than a demo,
# it has no `__main__` guard, and importing it reads the real datastore.
DEMO_MODULES = sorted(p.stem for p in SCRATCH_DIR.glob("scratch_*.py"))


def _run(args: list[str], tmp_path: Path) -> subprocess.CompletedProcess:
    """Run a demo in its own process, kept away from the real logs directory.

    The environment is inherited and then overridden, rather than built from a
    literal dict. The literal version named a POSIX `PATH` and guessed at
    `SYSTEMROOT`, which is a list that has to stay right on every platform the
    suite runs on — and the demos import `ccxt`, so `ssl` and `socket`, which
    on Windows do not load without the environment the interpreter was started
    with. Only the three values these tests actually control are set here.
    """
    return subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        env={
            **os.environ,
            "PAPER": "true",
            "TM_LOG_DIR": str(tmp_path / "logs"),
            "PYTHONIOENCODING": "utf-8",
        },
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )


class TestScratchPackageBootstrap:
    """The one-line reason `python -m scratch.<demo>` can find `log_demo`."""

    def test_scratch_is_a_package(self):
        assert (SCRATCH_DIR / "__init__.py").exists()

    def test_importing_it_puts_the_scratch_directory_on_the_path(self):
        import scratch  # noqa: F401  (imported for its side effect)

        assert str(SCRATCH_DIR.resolve()) in sys.path

    def test_it_also_puts_the_repo_root_on_the_path(self):
        """`config` and `logging_config` live there, and every demo imports them."""
        import scratch  # noqa: F401

        assert str(PROJECT_ROOT.resolve()) in sys.path

    def test_repeated_imports_do_not_stack_duplicate_entries(self):
        import importlib

        import scratch

        before = sys.path.count(str(SCRATCH_DIR.resolve()))
        importlib.reload(scratch)
        assert sys.path.count(str(SCRATCH_DIR.resolve())) == before

    def test_there_are_demos_to_check(self):
        """A glob that silently matches nothing would make the sweep vacuous."""
        assert len(DEMO_MODULES) >= 10


@pytest.mark.integration
class TestEveryDemoResolvesItsImports:
    """One subprocess, every demo — the import that used to fail is at module scope.

    Importing rather than running: the module bodies only import and define, so
    this exercises the broken line without letting fifteen demos hit temp
    directories and exchange APIs.
    """

    def test_all_demos_import_as_package_submodules(self, tmp_path):
        program = (
            "import importlib\n"
            f"for name in {DEMO_MODULES!r}:\n"
            "    importlib.import_module('scratch.' + name)\n"
        )
        result = _run([sys.executable, "-c", program], tmp_path)
        assert result.returncode == 0, result.stderr


@pytest.mark.integration
class TestTheReportedInvocation:
    """`python -m scratch.scratch_audit`, the command in the bug report.

    `scratch_audit` is the demo to run for real: it is offline, it builds its
    own store under a temp directory, and it finishes in a couple of seconds.
    """

    def test_running_a_demo_as_a_module_succeeds(self, tmp_path):
        result = _run([sys.executable, "-m", "scratch.scratch_audit"], tmp_path)

        assert result.returncode == 0, result.stderr
        assert "ModuleNotFoundError" not in result.stderr
        assert "Audit Demo Complete" in result.stdout

    def test_running_the_same_demo_as_a_script_still_succeeds(self, tmp_path):
        """The form the README documents; the fix must not have traded one for the other."""
        result = _run([sys.executable, str(SCRATCH_DIR / "scratch_audit.py")], tmp_path)

        assert result.returncode == 0, result.stderr
        assert "Audit Demo Complete" in result.stdout

    def test_the_demo_logged_to_the_component_file_either_way(self, tmp_path):
        """A demo that cannot reach `log_demo` cannot set a run_id or write a tail."""
        _run([sys.executable, "-m", "scratch.scratch_audit"], tmp_path)

        audit_log = tmp_path / "logs" / "audit.log"
        assert audit_log.exists(), "the demo produced no audit.log"
        assert audit_log.read_text().splitlines()


class TestTheEnvironmentAChildDemoGets:
    """`scratch_observability` runs the pipeline in a subprocess; that child
    needs a working interpreter environment, and hand-building one is how it
    stopped having one.

    The reported symptom was a `FileNotFoundError` on the log file the demo had
    just asked the child to write. The child had exited 1 without writing
    anything, because a four-key env dict (PATH, PAPER, TM_LOG_DIR, PYTHONPATH)
    left out everything else Windows needs to import `ssl` and `socket` — which
    `ccxt`, imported at `pipeline.nightly`'s module scope, pulls in. The
    diagnosis was in a stderr pipe the demo captured and never printed.

    These are in-process because the failure is in a dict, not in a process.
    """

    @staticmethod
    def _env(tmp_path: Path) -> dict[str, str]:
        from scratch.scratch_observability import child_env

        return child_env(tmp_path / "module-run")

    def test_it_inherits_the_parent_environment(self, tmp_path, monkeypatch):
        """The name is a stand-in for SYSTEMROOT, TEMP, and anything else a
        platform needs; the point is that the child is not handed a whitelist."""
        monkeypatch.setenv("TM_DEMO_CANARY", "inherited")

        assert self._env(tmp_path).get("TM_DEMO_CANARY") == "inherited"

    def test_it_still_overrides_the_three_values_the_demo_controls(self, tmp_path):
        monkeypatch_free = self._env(tmp_path)

        assert monkeypatch_free["PAPER"] == "true"
        assert monkeypatch_free["TM_LOG_DIR"] == str(tmp_path / "module-run")
        assert monkeypatch_free["PYTHONPATH"] == str(PROJECT_ROOT)

    def test_the_log_directory_override_wins_over_an_inherited_one(self, tmp_path, monkeypatch):
        """Otherwise the demo would write into whatever TM_LOG_DIR the shell
        already had — including the real `logs/`."""
        monkeypatch.setenv("TM_LOG_DIR", "/somewhere/else")

        assert self._env(tmp_path)["TM_LOG_DIR"] == str(tmp_path / "module-run")

    def test_the_child_is_told_to_write_utf8(self, tmp_path):
        """The pipeline's report prints `✓`; a captured stdout would otherwise
        take the locale encoding, which on Windows cannot encode it."""
        assert self._env(tmp_path)["PYTHONIOENCODING"] == "utf-8"


class TestTheDemoLetsGoOfItsTemporaryLogFiles:
    """Sections 2 and 3 point log files at a temp directory; a handler holds
    its file open, and Windows will not delete an open file.

    The demo therefore printed all five sections and then died in
    `TemporaryDirectory.__exit__` with `PermissionError: [WinError 32]` — it
    looked like the interpreter had failed rather than the demo. POSIX unlinks
    an open file happily, so a local run and every GitHub-hosted CI job were
    green; the self-hosted deploy gate, which is the Windows machine, was not.

    The assertion is the invariant rather than the platform: no `tm` logger may
    still hold an open handle under the directory that is about to be removed.
    """

    @staticmethod
    def _detach_all_tm_file_handlers() -> None:
        """The fixture's own teardown, written independently of the function
        under test.

        Not a duplicate for its own sake: `_ensure_component_handler` returns
        early when a component logger already has a file handler, so one test's
        leaked handler stops the next test from opening a file at all — and an
        assertion that no handle is open under `tmp_path` then passes for the
        wrong reason. Establishing the precondition with the code under test
        would hide exactly the regression these tests exist to catch.
        """
        names = ["tm", *(n for n in logging.root.manager.loggerDict if n.startswith("tm."))]
        for name in names:
            logger = logging.getLogger(name)
            for handler in list(logger.handlers):
                if isinstance(handler, logging.FileHandler):
                    logger.removeHandler(handler)
                    handler.close()

    @pytest.fixture(autouse=True)
    def _clean_logging_state(self):
        import logging_config
        from config import LOG_CONFIG

        self._detach_all_tm_file_handlers()
        yield
        self._detach_all_tm_file_handlers()
        logging_config.configure_logging(LOG_CONFIG, force=True)

    @staticmethod
    def _open_a_component_file_under(tmp_path: Path):
        """Reproduce what section 2 does — repoint the *active* config, then
        log — which is what makes later `get_logger` calls open files there
        too."""
        import logging_config
        from config import LogConfig

        cfg = LogConfig(dir=tmp_path / "levels", level="INFO")
        logging_config.configure_logging(cfg, force=True)
        logging_config.get_logger("signals.demo").info("a record")
        logging_config.get_logger("datastore.demo").info("import signals does this")
        return cfg

    @staticmethod
    def _open_handles_under(directory: Path) -> list[logging.Handler]:
        names = ["tm", *(n for n in logging.root.manager.loggerDict if n.startswith("tm."))]
        return [
            handler
            for name in names
            for handler in logging.getLogger(name).handlers
            if isinstance(handler, logging.FileHandler)
            and directory in Path(handler.baseFilename).parents
            and handler.stream is not None
        ]

    def test_the_leak_is_real_before_it_is_released(self, tmp_path):
        """A negative control: without this the test below could pass by
        asserting nothing was ever opened."""
        from scratch.scratch_observability import release_temp_log_handlers

        self._open_a_component_file_under(tmp_path)
        assert self._open_handles_under(tmp_path), "nothing held the temp files open"
        release_temp_log_handlers()

    def test_releasing_closes_every_handle_under_the_temp_directory(self, tmp_path):
        from scratch.scratch_observability import release_temp_log_handlers

        self._open_a_component_file_under(tmp_path)
        release_temp_log_handlers()

        assert not self._open_handles_under(tmp_path)

    def test_the_directory_can_then_be_removed(self, tmp_path):
        """The operation that actually failed, run for real. It is a no-op
        assertion on POSIX and the whole point on Windows."""
        import shutil

        from scratch.scratch_observability import release_temp_log_handlers

        self._open_a_component_file_under(tmp_path)
        release_temp_log_handlers()

        shutil.rmtree(tmp_path)
        assert not tmp_path.exists()

    def test_the_real_config_is_restored(self, tmp_path):
        """Otherwise the next `get_logger` in the process — including the
        atexit log tail — would still be writing into a deleted directory."""
        import logging_config
        from config import LOG_CONFIG
        from scratch.scratch_observability import release_temp_log_handlers

        self._open_a_component_file_under(tmp_path)
        release_temp_log_handlers()

        assert logging_config._active_cfg is LOG_CONFIG


@pytest.mark.integration
class TestTheObservabilityDemoRunsEndToEnd:
    """The demo in the bug report, run both documented ways.

    It is the one demo that starts a subprocess of its own, so it is the one
    where the parent's environment matters — and an import-only sweep cannot
    see that, exactly as an import-only test could not see `tm.__main__`.
    """

    def test_running_it_as_a_module_succeeds(self, tmp_path):
        result = _run([sys.executable, "-m", "scratch.scratch_observability"], tmp_path)

        assert result.returncode == 0, result.stdout + result.stderr
        assert "FAILED: no pipeline.log" not in result.stdout
        assert "record(s)" in result.stdout

    def test_the_child_pipeline_wrote_its_log(self, tmp_path):
        """The demo's actual claim: `python -m pipeline.nightly` leaves records
        in `pipeline.log`, under `tm.pipeline` rather than `tm.__main__`."""
        result = _run([sys.executable, "-m", "scratch.scratch_observability"], tmp_path)

        assert "exit code: 0" in result.stdout
        assert "logger:  tm.pipeline" in result.stdout
