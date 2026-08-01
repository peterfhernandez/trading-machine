"""Signal interface and registry.

A signal is a function `RebalanceContext -> {asset_id: score | None}` where a
higher score means "more attractive long" and `None` means "no view" (never
`0.0`, which is a view that the asset is exactly average).

The registry is how the backtester, the pipeline, and the breadth report find
signals without importing them sideways: each signal module registers itself,
and callers ask for `signal_functions()`.

Registration enforces the project rule that **every signal has a methodology
doc**: `register` raises if the declared doc is missing, so a signal cannot be
wired into the pipeline before its spec exists.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from config import PROJECT_ROOT
from logging_config import get_logger
from signals.methodology_doc import (
    MethodologyDoc,
    parse_methodology,
    undocumented_parameters,
)

log = get_logger(__name__)

SignalScores = Mapping[str, float | None]
SignalFn = Callable[..., SignalScores]

METHODOLOGY_DIR = Path(__file__).parent / "methodology"

FAMILIES = ("momentum", "carry", "reversal", "volatility", "value", "other")


@dataclass(frozen=True)
class Signal:
    """One registered signal.

    Attributes:
        signal_id: Registry key; must match the module name and the doc filename.
        family: Style family, used by the breadth report to group correlated bets.
        score_fn: `ctx -> {asset_id: score | None}`, higher = more attractive.
        methodology: Path to the signal's METHODOLOGY doc.
        params: The parameter object the `score_fn` was built with, for the record.
        doc: The parsed methodology doc — `None` only if parsing was skipped.
    """

    signal_id: str
    family: str
    score_fn: SignalFn
    methodology: Path
    params: object | None = field(default=None, compare=False)
    doc: MethodologyDoc | None = field(default=None, compare=False)

    @property
    def status(self) -> str:
        """`draft` / `backtested` / `live-paper` / `retired`, from the doc.

        A signal with no parsed doc reports `unknown` rather than `draft`: the
        two are different, and quietly reporting the optimistic one is how a
        signal ends up trading on the strength of a doc nobody read.
        """
        return self.doc.status if self.doc else "unknown"

    @property
    def is_evidenced(self) -> bool:
        """Whether §5 has been filled in — i.e. the signal has left `draft`."""
        return bool(self.doc and self.doc.is_evidenced)


_REGISTRY: dict[str, Signal] = {}


def methodology_path(signal_id: str) -> Path:
    """Conventional location of a signal's methodology doc."""
    return METHODOLOGY_DIR / f"{signal_id}.md"


def register(
    signal_id: str,
    family: str,
    score_fn: SignalFn,
    params: object | None = None,
    methodology: Path | None = None,
) -> Signal:
    """Register a signal, replacing any previous entry under the same id.

    Args:
        signal_id: Registry key (must match the module name).
        family: One of `FAMILIES`.
        score_fn: `ctx -> {asset_id: score | None}`.
        params: Parameter object used to build `score_fn`.
        methodology: Doc path; defaults to `methodology/<signal_id>.md`.

    Returns:
        The registered `Signal`.

    Raises:
        ValueError: If the family is unknown, the methodology doc is missing —
            the doc is the spec, so there is nothing to implement without it —
            or the doc disagrees with what is being registered (see below).

    The doc is not merely required to exist, it has to describe *this* signal:
    its `Signal ID` and `Family` header rows must match the registration, and
    every field of `params` must appear in its §4 parameter table. Existence
    alone let a doc drift into specifying a different signal from the one being
    run, which is the failure the check is for.
    """
    if family not in FAMILIES:
        raise ValueError(f"Unknown signal family {family!r}; expected one of {FAMILIES}")

    doc = methodology or methodology_path(signal_id)
    doc_label = doc.relative_to(PROJECT_ROOT) if doc.is_relative_to(PROJECT_ROOT) else doc
    if not doc.exists():
        log.warning(
            "Refusing to register %r: no methodology doc at %s", signal_id, doc_label
        )
        raise ValueError(
            f"Signal {signal_id!r} has no methodology doc at "
            f"{doc_label}; write the doc before registering the signal"
        )

    parsed = parse_methodology(doc)
    if parsed.signal_id != signal_id or parsed.family != family:
        log.warning(
            "Refusing to register %r: %s describes %r (family=%s)",
            signal_id, doc_label, parsed.signal_id, parsed.family,
        )
        raise ValueError(
            f"Methodology doc {doc_label} describes signal "
            f"{parsed.signal_id!r} (family={parsed.family!r}), but "
            f"{signal_id!r} (family={family!r}) is being registered"
        )

    undocumented = undocumented_parameters(parsed, params)
    if undocumented:
        log.warning(
            "Refusing to register %r: %s does not document %s",
            signal_id, doc_label, ", ".join(undocumented),
        )
        raise ValueError(
            f"Signal {signal_id!r} has parameters the methodology doc does not "
            f"document: {', '.join(undocumented)}. Add them to section 4 of "
            f"{doc_label} — the doc is the spec."
        )

    replaced = signal_id in _REGISTRY
    signal = Signal(
        signal_id=signal_id,
        family=family,
        score_fn=score_fn,
        methodology=doc,
        params=params,
        doc=parsed,
    )
    _REGISTRY[signal_id] = signal
    log.info(
        "Registered signal %s (family=%s, status=%s, doc=%s)%s",
        signal_id, family, parsed.status, doc_label,
        " [replacing existing]" if replaced else "",
    )
    return signal


def get(signal_id: str) -> Signal:
    """Look up a registered signal.

    Raises:
        KeyError: If no signal is registered under `signal_id`.
    """
    if signal_id not in _REGISTRY:
        raise KeyError(f"No signal registered as {signal_id!r}; known: {sorted(_REGISTRY)}")
    return _REGISTRY[signal_id]


def all_signals() -> dict[str, Signal]:
    """Every registered signal, keyed by id (a copy; mutating it changes nothing)."""
    return dict(_REGISTRY)


def signal_statuses() -> dict[str, str]:
    """`{signal_id: status}` for every registered signal.

    Phase 5's honest summary — "implemented and tested, not evidenced" — was a
    paragraph in the README that a human had to remember to update. This makes
    it a query: anything still reporting `draft` has no §5 numbers behind it.
    """
    return {signal_id: signal.status for signal_id, signal in _REGISTRY.items()}


def signal_functions(signal_ids: list[str] | None = None) -> dict[str, SignalFn]:
    """Named score functions, shaped for `Backtester.run(signals=...)`.

    Args:
        signal_ids: Which signals to include; None includes all registered.
    """
    ids = sorted(_REGISTRY) if signal_ids is None else signal_ids
    return {signal_id: get(signal_id).score_fn for signal_id in ids}
