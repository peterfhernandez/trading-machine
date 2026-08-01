"""Scratch script demonstrating that `.env` is loaded by the code itself.

Runs against the real repository root and the real `.env`, if there is one —
this is the demo whose value is that it uses the actual environment rather than
a fixture. It never prints a secret: key *names* and a set/unset verdict only.

What it shows:

1. Where `config.py` looks for the file, and whether it found one.
2. Which keys that file supplied, and which were already in the environment —
   `.env` fills gaps and never overrides, so the two lists are disjoint.
3. Which venue and alert credentials are consequently available, masked.
4. That the working directory does not change the answer: a child interpreter
   started from another directory, with a decoy `.env` beside it, loads the
   repository's file and ignores the decoy.

    PAPER=true python scratch/scratch_dotenv.py
    PAPER=true python -m scratch.scratch_dotenv
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from log_demo import start_demo_run  # noqa: E402

from config import ALERT_CONFIG, DOTENV_KEYS, DOTENV_PATH, PAPER, PROJECT_ROOT  # noqa: E402
from logging_config import get_logger  # noqa: E402

logger = get_logger("pipeline.dotenv_demo")

# Names only. A demo that prints a token into a terminal — and a scrollback, and
# possibly a screen share — has done more harm than the demonstration is worth.
CREDENTIAL_KEYS = [
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "DERIBIT_CLIENT_ID",
    "DERIBIT_CLIENT_SECRET",
]


def verdict(key: str) -> str:
    """`set (N chars)` or `unset` — never the value."""
    value = os.getenv(key)
    if value is None:
        return "unset"
    if value == "":
        return "set but empty"
    return f"set ({len(value)} chars)"


def demo_where_the_file_is_looked_for():
    print("\n" + "=" * 70)
    print("1. WHERE config.py LOOKS")
    print("=" * 70)

    print(f"\n   PROJECT_ROOT: {PROJECT_ROOT}")
    print(f"   DOTENV_PATH:  {DOTENV_PATH}")
    print(f"   exists:       {DOTENV_PATH.is_file()}")

    if not DOTENV_PATH.is_file():
        print(
            "\n   No .env at the project root. Everything below reflects the\n"
            "   environment this process was started with, which is the point:\n"
            "   the code loads the file when it is there and does not require it."
        )


def demo_which_keys_came_from_the_file():
    print("\n" + "=" * 70)
    print("2. WHAT THE FILE SUPPLIED (names only)")
    print("=" * 70)

    print(f"\n   Loaded from .env:  {sorted(DOTENV_KEYS) or '(none)'}")

    if DOTENV_PATH.is_file():
        # Present in the file but absent from the loaded list == the process
        # already had it, and the real environment wins.
        in_file = set()
        for raw in DOTENV_PATH.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                in_file.add(line.partition("=")[0].strip())
        already_set = sorted(in_file - set(DOTENV_KEYS))
        print(f"   Already in the environment (file yielded): {already_set or '(none)'}")


def demo_what_is_consequently_available():
    print("\n" + "=" * 70)
    print("3. CREDENTIALS NOW VISIBLE TO config.py")
    print("=" * 70)
    print()

    for key in CREDENTIAL_KEYS:
        source = " <- .env" if key in DOTENV_KEYS else ""
        print(f"   {key:<24} {verdict(key)}{source}")

    print(f"\n   ALERT_CONFIG.enabled: {ALERT_CONFIG.enabled}")
    if ALERT_CONFIG.enabled:
        print(
            "   Telegram alerts WILL be sent by anything that calls send_alerts()\n"
            "   in this environment — including the test suite, which exercises\n"
            "   the halt path against a fabricated audit failure."
        )

    logger.info(
        "dotenv demo: %d key(s) loaded from %s; alerts enabled=%s",
        len(DOTENV_KEYS),
        DOTENV_PATH,
        ALERT_CONFIG.enabled,
    )


def demo_the_working_directory_does_not_matter():
    print("\n" + "=" * 70)
    print("4. THE WORKING DIRECTORY DOES NOT CHANGE THE ANSWER")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        elsewhere = Path(tmpdir)
        (elsewhere / ".env").write_text(
            "TM_DOTENV_DECOY=loaded from the working directory\n", encoding="utf-8"
        )

        print(f"\n   Starting a child interpreter in: {elsewhere}")
        print("   with a decoy .env beside it setting TM_DOTENV_DECOY")

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import os, config; print(os.getenv('TM_DOTENV_DECOY', '(not set)'))",
            ],
            cwd=elsewhere,
            env={**os.environ, "PYTHONPATH": str(PROJECT_ROOT)},
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
        )

        if result.returncode != 0:
            print(f"\n   [!] child failed:\n{result.stderr}")
            return

        print(f"\n   Child read TM_DOTENV_DECOY as: {result.stdout.strip()}")
        print(
            "   '(not set)' is the correct answer: the file that was loaded is\n"
            "   the repository's, anchored to PROJECT_ROOT, not whichever .env\n"
            "   happens to sit in the directory the command was run from."
        )


def main():
    start_demo_run("pipeline")

    print("\n╔" + "=" * 68 + "╗")
    print("║" + "  .ENV LOADING DEMO".ljust(68) + "║")
    print("╚" + "=" * 68 + "╝")

    if not PAPER:
        print("\n[!] PAPER=False: Skipping demo (this is demo code, not production)")
        return

    demo_where_the_file_is_looked_for()
    demo_which_keys_came_from_the_file()
    demo_what_is_consequently_available()
    demo_the_working_directory_does_not_matter()

    print("\n" + "=" * 70)
    print("DEMO COMPLETE")
    print("=" * 70)
    print(
        "\nThe credentials reach config.py because config.py loads them, not\n"
        "because an editor happened to populate the environment first.\n"
    )


if __name__ == "__main__":
    main()
