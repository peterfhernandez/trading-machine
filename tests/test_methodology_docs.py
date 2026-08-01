"""The methodology doc is the spec — so the spec is checked against the code.

`registry.register` has always required the doc to *exist*. Existence is a weak
contract: it cannot see a doc whose header claims a different family from the
one being registered, and it cannot see a parameter that lives in the code and
appears nowhere in the doc. The second had actually happened —
`short_term_reversal` and `time_series_momentum` each ran with two parameters
(`max_gap_days`, `history_buffer_days`) their §4 tables never mentioned, and
`low_volatility` with one — so three of six "specs" described a different signal
from the one being backtested, and nothing failed.

These tests are the enforcement. They are deliberately structural (header
fields, parameter names, status enum) and never assert prose: a schema over the
hypothesis section would only invite writing to the schema.
"""

import dataclasses
import importlib
import pkgutil
from pathlib import Path

import pytest

import signals
from signals import methodology_doc as md
from signals import registry

DOC_DIR = Path(signals.__file__).parent / "methodology"


def write_doc(path: Path, **overrides) -> Path:
    """A minimal well-formed methodology doc, for the negative cases."""
    fields = {
        "signal_id": "probe",
        "family": "momentum",
        "status": "draft",
        "params": "| `lookback_days` | 90 | — | because |",
        **overrides,
    }
    path.write_text(
        f"# Methodology: {fields['signal_id']}\n\n"
        "| Field | Value |\n| --- | --- |\n"
        f"| Signal ID | `{fields['signal_id']}` |\n"
        f"| Family | {fields['family']} |\n"
        "| Author | peter |\n"
        f"| Status | {fields['status']} |\n"
        "| Status note | a note |\n\n"
        "## 1. Hypothesis\n\nwords\n\n"
        "## 4. Parameters\n\n"
        "| Parameter | Value | Range tested | Why this value |\n| --- | --- | --- | --- |\n"
        f"{fields['params']}\n\n"
        "## 5. Backtest evidence\n\nnot yet\n",
        encoding="utf-8",
    )
    return path


class TestParsing:
    def test_header_fields_are_read(self, tmp_path):
        doc = md.parse_methodology(write_doc(tmp_path / "probe.md"))

        assert (doc.signal_id, doc.family, doc.status) == ("probe", "momentum", "draft")
        assert doc.status_note == "a note"

    def test_parameters_are_read_in_document_order(self, tmp_path):
        path = write_doc(
            tmp_path / "probe.md",
            params="| `lookback_days` | 90 | | |\n| `skip_days` | 7 | | |",
        )
        assert md.parse_methodology(path).parameters == ("lookback_days", "skip_days")

    def test_an_annotated_parameter_name_still_parses(self, tmp_path):
        """`markov_mean_reversion` writes "| `n_states` (k) |" for section 3."""
        path = write_doc(tmp_path / "probe.md", params="| `n_states` (k) | 5 | | |")
        assert md.parse_methodology(path).parameters == ("n_states",)

    def test_only_the_parameters_section_is_scanned(self, tmp_path):
        """A backticked name in section 5's table is not a parameter."""
        path = tmp_path / "probe.md"
        write_doc(path)
        path.write_text(
            path.read_text() + "\n| `not_a_param` | 1 | | |\n", encoding="utf-8"
        )
        assert md.parse_methodology(path).parameters == ("lookback_days",)

    @pytest.mark.parametrize("status", md.STATUSES)
    def test_every_template_status_parses(self, tmp_path, status):
        path = write_doc(tmp_path / "probe.md", status=status)
        assert md.parse_methodology(path).status == status

    def test_free_text_in_the_status_row_is_rejected(self, tmp_path):
        """The row that used to read "draft (implemented + tested; ...)".

        Context belongs in `Status note`; the status itself has to be a value
        something can branch on.
        """
        path = write_doc(tmp_path / "probe.md", status="draft (implemented, mostly)")
        with pytest.raises(ValueError, match="not one of"):
            md.parse_methodology(path)

    def test_a_doc_with_no_header_table_is_rejected(self, tmp_path):
        path = tmp_path / "probe.md"
        path.write_text("# Methodology: probe\n\n## 1. Hypothesis\n\nwords\n")
        with pytest.raises(ValueError, match="missing"):
            md.parse_methodology(path)

    def test_a_missing_doc_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            md.parse_methodology(tmp_path / "nope.md")

    def test_is_evidenced_tracks_leaving_draft(self, tmp_path):
        draft = md.parse_methodology(write_doc(tmp_path / "a.md", status="draft"))
        done = md.parse_methodology(write_doc(tmp_path / "b.md", status="backtested"))

        assert not draft.is_evidenced
        assert done.is_evidenced


class TestUndocumentedParameters:
    @dataclasses.dataclass(frozen=True)
    class Params:
        lookback_days: int = 90
        max_gap_days: int = 1

    def test_a_code_parameter_missing_from_the_doc_is_reported(self, tmp_path):
        doc = md.parse_methodology(write_doc(tmp_path / "probe.md"))
        assert md.undocumented_parameters(doc, self.Params()) == ("max_gap_days",)

    def test_a_documented_parameter_with_no_field_is_allowed(self, tmp_path):
        """`markov_mean_reversion` documents `pooling` as considered and not
        implemented. Losing the ability to write that down would make the doc
        worse, so the check runs one way only."""
        doc = md.parse_methodology(
            write_doc(
                tmp_path / "probe.md",
                params=(
                    "| `lookback_days` | 90 | | |\n"
                    "| `max_gap_days` | 1 | | |\n"
                    "| `pooling` | per-asset | | not implemented |"
                ),
            )
        )
        assert md.undocumented_parameters(doc, self.Params()) == ()

    def test_no_params_object_is_not_an_error(self, tmp_path):
        doc = md.parse_methodology(write_doc(tmp_path / "probe.md"))
        assert md.undocumented_parameters(doc, None) == ()


class TestRegistryEnforcesTheDoc:
    def test_a_doc_describing_another_signal_is_refused(self, tmp_path):
        doc = write_doc(tmp_path / "probe.md", signal_id="something_else")
        with pytest.raises(ValueError, match="describes signal"):
            registry.register("probe", "momentum", lambda ctx: {}, methodology=doc)

    def test_a_family_mismatch_is_refused(self, tmp_path):
        doc = write_doc(tmp_path / "probe.md", family="reversal")
        with pytest.raises(ValueError, match="describes signal"):
            registry.register("probe", "momentum", lambda ctx: {}, methodology=doc)

    def test_an_undocumented_parameter_is_refused(self, tmp_path):
        @dataclasses.dataclass(frozen=True)
        class Params:
            lookback_days: int = 90
            secret_knob: float = 0.5

        doc = write_doc(tmp_path / "probe.md")
        with pytest.raises(ValueError, match="secret_knob"):
            registry.register(
                "probe", "momentum", lambda ctx: {}, params=Params(), methodology=doc
            )

    def test_nothing_partial_is_left_in_the_registry(self, tmp_path):
        doc = write_doc(tmp_path / "probe.md", family="reversal")
        with pytest.raises(ValueError):
            registry.register("probe", "momentum", lambda ctx: {}, methodology=doc)
        assert "probe" not in registry.all_signals()

    def test_a_matching_doc_registers_and_carries_its_status(self, tmp_path):
        doc = write_doc(tmp_path / "probe.md", status="backtested")
        try:
            signal = registry.register(
                "probe", "momentum", lambda ctx: {}, methodology=doc
            )
            assert signal.status == "backtested"
            assert signal.is_evidenced
        finally:
            registry._REGISTRY.pop("probe", None)


class TestTheRealSignalSet:
    """The six registered signals, against their six real docs."""

    def test_every_signal_module_is_registered(self):
        """A signal module that is never added to the registration loop has no
        doc requirement, no status, and no test — it is invisible."""
        modules = []
        for info in pkgutil.iter_modules([str(Path(signals.__file__).parent)]):
            module = importlib.import_module(f"signals.{info.name}")
            if hasattr(module, "SIGNAL_ID"):
                modules.append(module.SIGNAL_ID)

        assert sorted(modules) == sorted(registry.all_signals())

    def test_every_doc_matches_the_signal_it_documents(self):
        for signal_id, signal in registry.all_signals().items():
            assert signal.doc is not None, signal_id
            assert signal.doc.signal_id == signal_id
            assert signal.doc.family == signal.family
            assert signal.methodology.name == f"{signal_id}.md"

    def test_every_live_parameter_is_documented(self):
        for signal_id, signal in registry.all_signals().items():
            missing = md.undocumented_parameters(signal.doc, signal.params)
            assert missing == (), f"{signal_id} runs undocumented parameters: {missing}"

    def test_every_status_is_a_known_value(self):
        for signal_id, status in registry.signal_statuses().items():
            assert status in md.STATUSES, (signal_id, status)

    def test_all_six_are_still_draft(self):
        """Phase 5's honest summary, as an assertion rather than a paragraph.

        This test is *meant* to fail when a signal's §5 is filled in from a real
        backfill — that is the moment to update it, and having to is the point.
        """
        assert set(registry.signal_statuses().values()) == {"draft"}

    def test_the_template_is_not_mistaken_for_a_signal(self):
        assert (DOC_DIR / "TEMPLATE.md").exists()
        assert "TEMPLATE" not in registry.all_signals()

    def test_no_orphan_docs(self):
        """A doc with no signal behind it is a spec for nothing."""
        documented = {p.stem for p in DOC_DIR.glob("*.md")} - {"TEMPLATE"}
        assert documented == set(registry.all_signals())
