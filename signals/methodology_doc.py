"""Reading a signal's methodology doc as data, not just as prose.

The project rule is that the doc is the spec and the code follows it, and
`registry.register` already enforced *existence*. Existence alone turned out to
be a weak contract: it cannot notice a doc whose header says `Family | reversal`
while the module registers `momentum`, and it cannot notice a parameter that
exists in the code and appears nowhere in the doc. Both had happened —
`short_term_reversal` and `time_series_momentum` each carried two live
parameters (`max_gap_days`, `history_buffer_days`) that their §4 tables never
mentioned, so the "spec" silently described a different signal from the one
being backtested.

So the header table and the §4 parameter table are parsed, and the registry
checks them against what the module actually registers. What is deliberately
*not* parsed is the prose: the hypothesis, the failure modes and the review log
are for humans, and a schema over them would only invite writing to the schema.

    doc = parse_methodology(Path("signals/methodology/carry.md"))
    doc.signal_id, doc.family, doc.status   # ('carry', 'carry', 'draft')
    doc.parameters                          # ('lookback_days', ...)
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from logging_config import get_logger

log = get_logger(__name__)

STATUSES: tuple[str, ...] = ("draft", "backtested", "live-paper", "retired")
"""The status lifecycle from TEMPLATE.md. `draft` means implemented at best;
nothing leaves `draft` without §5 backtest evidence."""

EVIDENCE_SECTION = "5. Backtest evidence"

_HEADER_ROW = re.compile(r"^\|\s*([^|]+?)\s*\|\s*(.*?)\s*\|\s*$")
# The first cell is the parameter name in backticks, optionally annotated —
# `markov_mean_reversion`'s table writes "| `n_states` (k) |" to carry the
# symbol used in section 3's formulas.
_PARAM_NAME = re.compile(r"^\|\s*`([A-Za-z_][A-Za-z0-9_]*)`")
_PARAMETER_SECTION = re.compile(r"^##\s*4\.\s*Parameters\s*$")
_NEXT_SECTION = re.compile(r"^##\s")


@dataclass(frozen=True)
class MethodologyDoc:
    """The machine-readable part of a signal's methodology doc.

    Attributes:
        path: Where the doc lives.
        signal_id: The `Signal ID` header row, backticks stripped.
        family: The `Family` header row.
        status: One of `STATUSES`, parsed from the `Status` header row.
        status_note: The free-text `Status note` row, or "".
        parameters: Names from the §4 table, in document order.
    """

    path: Path
    signal_id: str
    family: str
    status: str
    status_note: str
    parameters: tuple[str, ...]

    @property
    def is_evidenced(self) -> bool:
        """True once the signal has left `draft` — i.e. §5 was filled in.

        The one place the status is load-bearing rather than decorative: it is
        what a Phase 6+ pipeline should consult before allocating to a signal,
        instead of a human remembering which of six docs has numbers in it.
        """
        return self.status != "draft"


def _header_fields(lines: list[str]) -> dict[str, str]:
    """Rows of the leading `| Field | Value |` table, lowercased keys."""
    fields: dict[str, str] = {}
    for line in lines:
        if line.startswith("## "):
            break
        match = _HEADER_ROW.match(line)
        if not match:
            continue
        key, value = match.group(1).strip().lower(), match.group(2).strip()
        if key in ("field", "---") or set(key) <= {"-", " "}:
            continue
        fields[key] = value
    return fields


def _parameter_names(lines: list[str]) -> tuple[str, ...]:
    """Backticked names in the first column of the §4 table, in order."""
    names: list[str] = []
    inside = False
    for line in lines:
        if _PARAMETER_SECTION.match(line):
            inside = True
            continue
        if inside and _NEXT_SECTION.match(line):
            break
        if not inside:
            continue
        match = _PARAM_NAME.match(line)
        if match and match.group(1) not in names:
            names.append(match.group(1))
    return tuple(names)


def parse_methodology(path: Path) -> MethodologyDoc:
    """Parse a methodology doc's header table and §4 parameter table.

    Args:
        path: Path to `signals/methodology/<signal_id>.md`.

    Returns:
        The parsed `MethodologyDoc`.

    Raises:
        FileNotFoundError: If the doc does not exist.
        ValueError: If the header table has no `Signal ID`/`Family`/`Status`,
            or the status is not one of `STATUSES` — an unparseable header is a
            doc that no longer specifies anything, which is worse than a
            missing one because it looks fine.
    """
    text = Path(path).read_text(encoding="utf-8")
    lines = text.splitlines()

    fields = _header_fields(lines)
    missing = [f for f in ("signal id", "family", "status") if f not in fields]
    if missing:
        raise ValueError(
            f"{path}: methodology header table is missing {missing}; "
            f"copy the table from signals/methodology/TEMPLATE.md"
        )

    status = fields["status"].strip().strip("`").lower()
    if status not in STATUSES:
        raise ValueError(
            f"{path}: status {fields['status']!r} is not one of {STATUSES}. "
            f"Free-text context belongs in the `Status note` row."
        )

    return MethodologyDoc(
        path=Path(path),
        signal_id=fields["signal id"].strip().strip("`"),
        family=fields["family"].strip().strip("`"),
        status=status,
        status_note=fields.get("status note", ""),
        parameters=_parameter_names(lines),
    )


def undocumented_parameters(doc: MethodologyDoc, params: object) -> tuple[str, ...]:
    """Fields of `params` that the doc's §4 table does not mention.

    The check runs one way only. A parameter in the code and not in the doc is
    a spec that describes a different signal from the one being run. A row in
    the doc with no field behind it is usually a deliberate open question —
    `markov_mean_reversion`'s `pooling` is documented as considered and not
    implemented — and losing the ability to write that down would make the doc
    worse, not better.
    """
    import dataclasses

    if params is None or not dataclasses.is_dataclass(params):
        return ()
    documented = set(doc.parameters)
    return tuple(f.name for f in dataclasses.fields(params) if f.name not in documented)
