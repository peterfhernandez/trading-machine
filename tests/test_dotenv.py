"""`.env` has to be loaded by the code, not by whatever launched it.

The symptom that started this: running the test suite locally sent a real
Telegram message to the trading machine's bot, and running it in CI did not.
`config.py` reads `TELEGRAM_BOT_TOKEN` with `os.getenv` and nothing in the
repository ever read a `.env` file — the credentials were reaching the process
because the editor was loading `.env` into the environment it started pytest
in. Same command, same checkout, different behaviour depending on how the
interpreter was launched, which is the same class of defect as the
`python -m pipeline.nightly` logger and the `python -m scratch.<demo>` import.

Two properties carry the weight here, and neither is visible in-process:

1. **The file is found relative to `PROJECT_ROOT`, not the working directory.**
   A CWD-relative load works from the repo root and silently does nothing from
   anywhere else — and a scheduled job's working directory is rarely the one
   you developed in.
2. **The load happens before the dataclass defaults are evaluated.**
   `AlertConfig.telegram_bot_token` is a *default*, computed when the class
   body executes at import. Move `_load_dotenv` below that class and every
   unit test here still passes while the feature does nothing.

The subprocess tests build a fake project root — a copy of `config.py` beside
a `.env` — and import it from a different working directory. That keeps them
away from the developer's real `.env`, which a test must never read and must
certainly never write over.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import _load_dotenv  # noqa: E402


@pytest.fixture(autouse=True)
def restore_environment():
    """`_load_dotenv` mutates `os.environ`; monkeypatch cannot undo what it adds."""
    saved = dict(os.environ)
    yield
    os.environ.clear()
    os.environ.update(saved)


def write_env(tmp_path: Path, body: str) -> Path:
    path = tmp_path / ".env"
    path.write_text(body, encoding="utf-8")
    return path


class TestParsing:
    """A `KEY=VALUE` file, not a shell script."""

    def test_a_plain_assignment_reaches_os_environ(self, tmp_path):
        loaded = _load_dotenv(write_env(tmp_path, "TM_TEST_KEY=abc123\n"))

        assert os.environ["TM_TEST_KEY"] == "abc123"
        assert loaded == ["TM_TEST_KEY"]

    def test_a_missing_file_is_not_an_error(self, tmp_path):
        assert _load_dotenv(tmp_path / "nothing-here") == []

    def test_comments_and_blank_lines_are_skipped(self, tmp_path):
        loaded = _load_dotenv(
            write_env(tmp_path, "# a comment\n\n   \nTM_TEST_KEY=value\n")
        )

        assert loaded == ["TM_TEST_KEY"]

    def test_surrounding_whitespace_is_stripped(self, tmp_path):
        _load_dotenv(write_env(tmp_path, "  TM_TEST_KEY  =  value  \n"))

        assert os.environ["TM_TEST_KEY"] == "value"

    @pytest.mark.parametrize("quote", ["'", '"'])
    def test_matched_quotes_are_stripped(self, tmp_path, quote):
        _load_dotenv(write_env(tmp_path, f"TM_TEST_KEY={quote}spaced value{quote}\n"))

        assert os.environ["TM_TEST_KEY"] == "spaced value"

    def test_an_unmatched_quote_is_left_alone(self, tmp_path):
        """A token that genuinely starts with a quote is not a quoted string."""
        _load_dotenv(write_env(tmp_path, "TM_TEST_KEY='abc\n"))

        assert os.environ["TM_TEST_KEY"] == "'abc"

    def test_a_value_may_contain_equals_signs(self, tmp_path):
        """Base64 padding and URLs both end in `=`; splitting on the last one loses data."""
        _load_dotenv(write_env(tmp_path, "TM_TEST_KEY=a=b=c==\n"))

        assert os.environ["TM_TEST_KEY"] == "a=b=c=="

    def test_an_empty_value_is_kept(self, tmp_path):
        _load_dotenv(write_env(tmp_path, "TM_TEST_KEY=\n"))

        assert os.environ["TM_TEST_KEY"] == ""

    def test_a_malformed_line_is_skipped_rather_than_raised_on(self, tmp_path):
        """A typo in `.env` must not stop the machine from starting."""
        loaded = _load_dotenv(
            write_env(tmp_path, "this line has no equals sign\n=novalue\nTM_TEST_KEY=ok\n")
        )

        assert loaded == ["TM_TEST_KEY"]


class TestTheRealEnvironmentWins:
    """`.env` fills gaps. It never overrides what the process was started with.

    `deploy.yml` sets `PAPER` and `TM_LOG_DIR` for its verification run on the
    trading machine, where the repo root also holds a real `.env`. If the file
    won, the verification run would write to the machine's live log directory.
    """

    def test_an_existing_variable_is_not_replaced(self, tmp_path):
        os.environ["TM_TEST_KEY"] = "from the environment"

        loaded = _load_dotenv(write_env(tmp_path, "TM_TEST_KEY=from the file\n"))

        assert os.environ["TM_TEST_KEY"] == "from the environment"
        assert loaded == [], "a key that was already set was reported as loaded"

    def test_an_existing_empty_variable_still_wins(self, tmp_path):
        """Deliberately blanking a variable is a way of switching a feature off."""
        os.environ["TM_TEST_KEY"] = ""

        _load_dotenv(write_env(tmp_path, "TM_TEST_KEY=from the file\n"))

        assert os.environ["TM_TEST_KEY"] == ""

    def test_the_other_keys_in_the_file_still_load(self, tmp_path):
        os.environ["TM_TEST_KEY"] = "from the environment"

        loaded = _load_dotenv(
            write_env(tmp_path, "TM_TEST_KEY=from the file\nTM_TEST_OTHER=loaded\n")
        )

        assert loaded == ["TM_TEST_OTHER"]


class TestWhereTheFileIsLookedFor:
    def test_the_path_is_anchored_to_the_project_root(self):
        import config

        assert config.DOTENV_PATH == config.PROJECT_ROOT / ".env"
        assert config.DOTENV_PATH.is_absolute()


# ---------------------------------------------------------------------------
# The properties only a subprocess can see
# ---------------------------------------------------------------------------
#
# Both of these are about *how the interpreter was started* — its working
# directory, and the order in which module-level statements ran. The test
# process has already imported `config`, from the repo root, so nothing
# in-process can observe either one.


def fake_project_root(tmp_path: Path, env_body: str) -> Path:
    """A copy of `config.py` beside a `.env`, so the real one is never touched."""
    root = tmp_path / "fake-root"
    root.mkdir()
    shutil.copy(PROJECT_ROOT / "config.py", root / "config.py")
    (root / ".env").write_text(env_body, encoding="utf-8")
    return root


def run_python(
    code: str, cwd: Path, root: Path, unset: tuple[str, ...] = (), **env_overrides
) -> subprocess.CompletedProcess:
    """Import config from `root`, with `cwd` deliberately somewhere else.

    The environment is inherited and then overridden rather than built from a
    literal dict: on Windows `ssl` and `socket` do not load without the
    environment the interpreter was started with. `unset` removes a variable
    outright, which is not the same as setting it empty — an empty variable is
    still present, and `.env` yields to anything present.
    """
    env = {**os.environ, "PYTHONPATH": str(root), **env_overrides}
    for key in unset:
        env.pop(key, None)
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=cwd,
        env=env,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=120,
    )


@pytest.mark.integration
class TestLoadedByTheCodeNotTheLauncher:
    def test_a_dotenv_at_the_project_root_is_loaded(self, tmp_path):
        root = fake_project_root(tmp_path, "TM_TEST_KEY=from the file\n")
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()

        result = run_python(
            "import os, config; print(os.environ.get('TM_TEST_KEY'))",
            cwd=elsewhere,
            root=root,
            unset=("TM_TEST_KEY",),
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "from the file"

    def test_the_working_directory_is_not_where_it_looks(self, tmp_path):
        """The regression: a CWD-relative load is right exactly when you test it.

        A `.env` sitting in the directory the command was run from belongs to
        something else, and reading it would make behaviour depend on where you
        happened to be standing.
        """
        root = fake_project_root(tmp_path, "TM_TEST_KEY=from the project root\n")
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        (elsewhere / ".env").write_text("TM_TEST_KEY=from the cwd\n", encoding="utf-8")

        result = run_python(
            "import os, config; print(os.environ.get('TM_TEST_KEY'))",
            cwd=elsewhere,
            root=root,
            unset=("TM_TEST_KEY",),
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "from the project root"

    def test_the_real_environment_still_wins_end_to_end(self, tmp_path):
        root = fake_project_root(tmp_path, "TM_TEST_KEY=from the file\n")

        result = run_python(
            "import os, config; print(os.environ.get('TM_TEST_KEY'))",
            cwd=tmp_path,
            root=root,
            TM_TEST_KEY="from the environment",
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "from the environment"


@pytest.mark.integration
class TestTheLoadHappensBeforeTheDefaultsAreEvaluated:
    """The ordering property, pinned by its actual consumer.

    `AlertConfig`'s fields are dataclass defaults, so they are computed when the
    class body runs. A `_load_dotenv` call placed below that class parses the
    file correctly, sets `os.environ` correctly, passes every unit test above —
    and `ALERT_CONFIG.enabled` is still `False`, because the value was read
    before the file was loaded.
    """

    ENV = "TELEGRAM_BOT_TOKEN=fake-token\nTELEGRAM_CHAT_ID=fake-chat\n"

    def test_alert_config_sees_credentials_from_the_file(self, tmp_path):
        root = fake_project_root(tmp_path, self.ENV)

        result = run_python(
            "import config; print(config.ALERT_CONFIG.enabled)",
            cwd=tmp_path,
            root=root,
            unset=("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"),
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "True"

    def test_without_the_file_it_stays_disabled(self, tmp_path):
        """The negative control: `True` above has to be the file's doing."""
        root = fake_project_root(tmp_path, "")

        result = run_python(
            "import config; print(config.ALERT_CONFIG.enabled)",
            cwd=tmp_path,
            root=root,
            unset=("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"),
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == "False"
