"""A passing suite must exit 0, including through pytest's temp-dir housekeeping.

The regression: on Windows, `python -m pytest tests/` printed every test as
PASSED and then died with

    File "_pytest/pathlib.py", line 357, in cleanup_dead_symlinks
      left_dir.unlink()
  PermissionError: [WinError 5] Access is denied:
      '...\\Temp\\pytest-of-Peter\\pytest-current'

`pytest-current` is the best-effort "latest run" link pytest keeps beside its
numbered temp directories. `_force_symlink` fails to re-point it on Windows
(unlinking a *directory* link needs `RemoveDirectoryW`), so it goes stale, its
target is eventually cleaned up, and `cleanup_dead_symlinks` then calls the same
failing `unlink()` on it — this time unguarded. Session-finish hooks run in
`wrap_session`'s `finally`, which catches only `exit.Exception`, so it escapes
and the process exits 1 on a suite where nothing failed.

Exit code 1 is not cosmetic: `.github/workflows/deploy.yml` throws on
`$LASTEXITCODE -ne 0`, so the trading machine keeps its old checkout rather than
adopting a commit that actually passed.

`tests/conftest.py` replaces the cleanup with one that cannot raise and knows
the Windows removal path. These tests pin that shim: that it is installed at
both call sites, that it still removes what pytest removed and leaves alone what
pytest left alone, and that it survives an `unlink` that refuses.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests.conftest import (
    _PATCHED_SYMLINK_CLEANUP_MODULES,
    PROJECT_ROOT,
    _remove_dangling_link,
    _tolerant_cleanup_dead_symlinks,
)


@pytest.fixture
def link_root(tmp_path):
    """A stand-in for `<tmp>/pytest-of-<user>/`."""
    root = tmp_path / "pytest-of-tester"
    root.mkdir()
    return root


def _dangling_dir_link(root: Path, name: str = "pytest-current") -> Path:
    """A directory link whose target has been cleaned up — the failing case."""
    target = root / "pytest-0"
    target.mkdir()
    link = root / name
    link.symlink_to(target, target_is_directory=True)
    target.rmdir()
    return link


class TestTheShimIsInstalled:
    def test_both_call_sites_are_patched(self):
        """`_pytest.tmpdir` binds the name at import; patching pathlib alone misses it."""
        assert _PATCHED_SYMLINK_CLEANUP_MODULES == ("_pytest.pathlib", "_pytest.tmpdir")

    @pytest.mark.parametrize("module_name", ["_pytest.pathlib", "_pytest.tmpdir"])
    def test_the_name_resolves_to_our_function(self, module_name):
        import importlib

        module = importlib.import_module(module_name)

        assert module.cleanup_dead_symlinks is _tolerant_cleanup_dead_symlinks

    def test_the_session_finish_path_goes_through_it(self, link_root, monkeypatch):
        """The traceback's actual call chain: cleanup_numbered_dir -> the cleanup."""
        import _pytest.pathlib as pytest_pathlib

        seen = []
        monkeypatch.setattr(pytest_pathlib, "cleanup_dead_symlinks", seen.append)

        pytest_pathlib.cleanup_numbered_dir(link_root, "pytest-", 3, 0.0)

        assert seen == [link_root]


class TestItStillDoesPytestsJob:
    def test_a_dangling_link_is_removed(self, link_root):
        link = _dangling_dir_link(link_root)

        _tolerant_cleanup_dead_symlinks(link_root)

        assert not link.is_symlink()
        assert list(link_root.iterdir()) == []

    def test_a_live_link_is_left_alone(self, link_root):
        target = link_root / "pytest-1"
        target.mkdir()
        link = link_root / "pytest-current"
        link.symlink_to(target, target_is_directory=True)

        _tolerant_cleanup_dead_symlinks(link_root)

        assert link.is_symlink()
        assert link.resolve() == target.resolve()

    def test_a_real_directory_is_never_touched(self, link_root):
        """Numbered dirs are cleaned up by lock, not by this function."""
        numbered = link_root / "pytest-2"
        numbered.mkdir()
        (numbered / "data.parquet").write_text("x")

        _tolerant_cleanup_dead_symlinks(link_root)

        assert (numbered / "data.parquet").exists()

    def test_a_missing_root_is_not_an_error(self, tmp_path):
        _tolerant_cleanup_dead_symlinks(tmp_path / "never-created")


class TestItCannotFailTheRun:
    def test_a_refused_unlink_falls_back_to_rmdir(self, link_root, monkeypatch):
        """Windows in miniature: DeleteFileW refuses, RemoveDirectoryW works.

        POSIX cannot produce the real WinError 5, so `unlink` is made to refuse
        and `rmdir` to do what `RemoveDirectoryW` does — drop the link, not the
        target. What is under test is the fallback, not the OS.
        """
        link = _dangling_dir_link(link_root)
        rmdir_calls = []

        def refuse_unlink(self, **kwargs):
            raise PermissionError(5, "Access is denied")

        def windows_rmdir(self):
            rmdir_calls.append(self)
            os.unlink(self)

        monkeypatch.setattr(Path, "unlink", refuse_unlink)
        monkeypatch.setattr(Path, "rmdir", windows_rmdir)

        _tolerant_cleanup_dead_symlinks(link_root)

        assert rmdir_calls == [link]
        assert list(link_root.iterdir()) == []

    def test_an_undeletable_link_does_not_raise(self, link_root, monkeypatch):
        """A link held open by another process must not take the exit code with it."""
        _dangling_dir_link(link_root)

        def refuse(self, *args, **kwargs):
            raise PermissionError(5, "Access is denied")

        monkeypatch.setattr(Path, "unlink", refuse)
        monkeypatch.setattr(Path, "rmdir", refuse)

        _tolerant_cleanup_dead_symlinks(link_root)  # must not raise

    def test_an_unreadable_entry_is_skipped(self, link_root, monkeypatch):
        """`resolve()` can raise too — pytest's version lets that escape as well."""

        def refuse(self, *args, **kwargs):
            raise PermissionError(5, "Access is denied")

        _dangling_dir_link(link_root)
        monkeypatch.setattr(Path, "resolve", refuse)

        _tolerant_cleanup_dead_symlinks(link_root)  # must not raise

    def test_remove_dangling_link_swallows_everything(self, tmp_path):
        _remove_dangling_link(tmp_path / "not-there")  # must not raise

    def test_pytest_itself_still_unlinks_without_a_guard(self):
        """Pins the defect, so the shim's removal is a deliberate decision.

        If a future pytest guards this itself, this test fails and the shim in
        `conftest.py` can go.
        """
        import inspect

        import _pytest.pathlib as pytest_pathlib

        source = inspect.getsource(pytest_pathlib).split("def cleanup_dead_symlinks")[1]
        body = source.split("\ndef ")[0]

        assert "unlink()" in body, "pytest no longer unlinks here — recheck the shim"
        assert "try" not in body, "pytest now guards this itself — the shim can be removed"


# The failure this file exists for is an *exit code*, and no in-process
# assertion can see one. Phase 5.6 already paid for that lesson: every
# import-based test of the logging retrofit passed while `python -m
# pipeline.nightly` logged nothing. So the last two tests run pytest for real.
#
# Windows is simulated by one line — `Path.unlink` refusing on `pytest-current`
# — because that is precisely what `os.unlink` does to a directory link there.
# Everything else is the genuine article: pytest's own temp-root layout, its own
# `_force_symlink` swallowing the failure, its own session-finish cleanup.

_REFUSE_UNLINK = """
    from pathlib import Path

    _real_unlink = Path.unlink

    def _windows_ish_unlink(self, *args, **kwargs):
        # A link to a *directory* needs RemoveDirectoryW on Windows; DeleteFileW
        # answers WinError 5, which is what os.unlink calls.
        if self.name == "pytest-current":
            raise PermissionError(5, "Access is denied")
        return _real_unlink(self, *args, **kwargs)

    Path.unlink = _windows_ish_unlink
"""

_INSTALL_SHIM = f"""
    import sys

    sys.path.insert(0, {str(PROJECT_ROOT)!r})
    from tests.conftest import _install_tolerant_symlink_cleanup

    _install_tolerant_symlink_cleanup()
"""


def _pytest_temp_user() -> str:
    """The name pytest puts in `pytest-of-<user>`.

    Guessing it from `$USER` is how the negative control below first came back
    green: the harness seeded a stale link under a directory pytest never
    looked at, and nothing crashed for the wrong reason.
    """
    import getpass

    try:
        return getpass.getuser() or "unknown"
    except (ImportError, OSError, KeyError):  # pragma: no cover - same fallback as pytest
        return "unknown"


def _run_pytest_over_a_stale_link(tmp_path: Path, conftest: str) -> subprocess.CompletedProcess:
    """One green test, run against a temp root holding a dangling `pytest-current`."""
    project = tmp_path / "project"
    project.mkdir()
    (project / "conftest.py").write_text(textwrap.dedent(conftest))
    (project / "test_green.py").write_text("def test_green(tmp_path):\n    assert tmp_path.exists()\n")

    # pytest resolves its root as <temproot>/pytest-of-<user>. The link points at
    # a numbered directory that has been cleaned up — the state a Windows box
    # ends up in, because `_force_symlink` could never re-point it.
    temproot = tmp_path / "temproot"
    root = temproot / f"pytest-of-{_pytest_temp_user()}"
    root.mkdir(parents=True, mode=0o700)
    (root / "pytest-current").symlink_to(root / "pytest-99", target_is_directory=True)

    env = {**os.environ, "PYTEST_DEBUG_TEMPROOT": str(temproot)}
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "test_green.py"],
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.integration
class TestTheExitCode:
    def test_a_green_run_exits_zero_with_the_shim(self, tmp_path):
        result = _run_pytest_over_a_stale_link(tmp_path, _REFUSE_UNLINK + _INSTALL_SHIM)

        assert "1 passed" in result.stdout
        assert result.returncode == 0, result.stdout + result.stderr
        assert "PermissionError" not in result.stderr

    def test_without_the_shim_the_same_green_run_exits_one(self, tmp_path):
        """Without this, the test above could pass because the setup is inert.

        It already earned that: the first version of the harness seeded the
        stale link under the wrong `pytest-of-<user>` directory, nothing
        crashed, and the positive test was green for no reason.

        Note what is missing from stdout — the `1 passed` summary. The crash
        happens *inside* the session-finish hook chain, below the terminal
        reporter's own hook, so the run's own verdict never gets printed. That
        is why the reported output jumps straight from the last PASSED line to
        a traceback.
        """
        result = _run_pytest_over_a_stale_link(tmp_path, _REFUSE_UNLINK)

        assert "[100%]" in result.stdout
        assert "failed" not in result.stdout
        assert result.returncode == 1
        assert "cleanup_dead_symlinks" in result.stderr
        assert "PermissionError" in result.stderr
