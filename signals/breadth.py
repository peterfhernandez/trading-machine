"""Breadth check: are these signals independent bets, or one bet with extra steps?

The fundamental law of active management is `IR ~ skill x sqrt(breadth)`, and
breadth counts *independent* bets. Five signals that agree with each other are
one signal with five names — they add turnover and complexity without adding
the square root. PLAN.md §6 principle 3 makes this a standing requirement, and
`signals/methodology/TEMPLATE.md` §6 asks every new signal to report its
correlation against everything already registered. This module is what produces
those numbers.

Two different correlations are reported, because they answer different
questions:

- **Score correlation** — per rebalance, the rank correlation between two
  signals' cross-sections, averaged over rebalances. This says whether the two
  signals *pick the same assets*. It is the one that decides whether a new
  signal is a rotation of an existing one.
- **IC correlation** — the correlation of the two signals' IC time series. This
  says whether they *work at the same times*. Two signals can pick different
  assets and still be one bet if they both stop working in the same regime, and
  a portfolio built on score correlation alone would not see it coming.

`effective_breadth` converts an average pairwise correlation into a count of
independent bets, `n / (1 + (n - 1) * rho_bar)`: five signals with an average
correlation of 0.5 are worth three, not five.

**No `backtest` import.** Signals are consumed by the backtester, so the
dependency arrow points one way (PLAN.md, Phase 5 notes); the rank correlation
here is a small local implementation rather than a reuse of
`backtest.metrics.spearman_ic`, and a test asserts the two agree.
"""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

import polars as pl

from logging_config import get_logger

log = get_logger(__name__)

REDUNDANCY_THRESHOLD = 0.7
"""Above this score correlation, two signals need a justification to both stay."""

SignalFn = Callable[[object], Mapping[str, float | None]]


# ---------------------------------------------------------------------------
# Rank correlation (local; see the module docstring for why it is not imported)
# ---------------------------------------------------------------------------


def _ranks(values: Sequence[float]) -> list[float]:
    """Average ranks, ties shared — the standard Spearman tie correction."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        shared = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = shared
        i = j + 1
    return ranks


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """Pearson correlation, or None when it is undefined (n < 2 or no dispersion)."""
    n = len(xs)
    if n < 2 or n != len(ys):
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    dx = [x - mean_x for x in xs]
    dy = [y - mean_y for y in ys]
    denominator = (sum(v * v for v in dx) ** 0.5) * (sum(v * v for v in dy) ** 0.5)
    if denominator <= 1e-12:
        return None
    return sum(a * b for a, b in zip(dx, dy, strict=True)) / denominator


def spearman(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """Rank correlation, or None when it is undefined."""
    if len(xs) < 2 or len(xs) != len(ys):
        return None
    return pearson(_ranks(xs), _ranks(ys))


# ---------------------------------------------------------------------------
# Recording scores during a backtest
# ---------------------------------------------------------------------------


class ScoreRecorder:
    """Wraps signal functions so a backtest run also captures their scores.

    The engine already calls each named signal once per rebalance to build the
    IC series; this rides along on that call rather than re-scoring the
    universe a second time (which for a multi-year run is the difference
    between one pass and two).

    ```python
    recorder = ScoreRecorder(signal_functions())
    result = Backtester(store).run(strategy, signals=recorder.wrapped())
    report = breadth_report(recorder.frame(), result.ic)
    ```
    """

    def __init__(self, signals: Mapping[str, SignalFn]):
        self._signals = dict(signals)
        self._rows: list[dict] = []

    def wrapped(self) -> dict[str, SignalFn]:
        """The same signals, each recording what it returns. Pass to `run(signals=...)`."""

        def record(name: str, fn: SignalFn) -> SignalFn:
            def wrapper(ctx):
                scores = fn(ctx)
                asof = getattr(ctx, "asof", None)
                for asset_id, score in scores.items():
                    if score is None:
                        continue
                    self._rows.append(
                        {
                            "rebalance_ts": asof,
                            "signal": name,
                            "asset_id": asset_id,
                            "score": float(score),
                        }
                    )
                return scores

            return wrapper

        return {name: record(name, fn) for name, fn in self._signals.items()}

    def frame(self) -> pl.DataFrame:
        """Recorded scores as `(rebalance_ts, signal, asset_id, score)`."""
        return score_frame(self._rows)


def score_frame(rows: Sequence[dict]) -> pl.DataFrame:
    """Build the canonical score frame, empty-safe."""
    schema = {
        "rebalance_ts": pl.Datetime("us"),
        "signal": pl.Utf8,
        "asset_id": pl.Utf8,
        "score": pl.Float64,
    }
    return pl.DataFrame(list(rows), schema=schema)


# ---------------------------------------------------------------------------
# Correlation matrices
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PairCorrelation:
    """One pair of signals' correlation, and how much data it rests on.

    Attributes:
        signal_a: First signal id (alphabetically first of the pair).
        signal_b: Second signal id.
        correlation: The correlation, or None when it could not be computed.
        n_observations: Rebalances (score correlation) or IC periods (IC
            correlation) the estimate used.
    """

    signal_a: str
    signal_b: str
    correlation: float | None
    n_observations: int


def score_correlations(scores: pl.DataFrame) -> list[PairCorrelation]:
    """Mean per-rebalance rank correlation between every pair of signals.

    Computed per rebalance over the assets **both** signals scored, then
    averaged across rebalances. Pooling every date into one correlation instead
    would let the cross-sectional level differences between dates dominate the
    number, which is not what "do these pick the same assets" means.

    Args:
        scores: Frame with `rebalance_ts`, `signal`, `asset_id`, `score`.

    Returns:
        One `PairCorrelation` per unordered pair, sorted by name.
    """
    if len(scores) == 0:
        return []

    by_date: dict[datetime, dict[str, dict[str, float]]] = {}
    for row in scores.iter_rows(named=True):
        by_date.setdefault(row["rebalance_ts"], {}).setdefault(row["signal"], {})[
            row["asset_id"]
        ] = row["score"]

    names = sorted(set(scores["signal"].to_list()))
    out: list[PairCorrelation] = []

    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            values: list[float] = []
            for per_signal in by_date.values():
                if a not in per_signal or b not in per_signal:
                    continue
                shared = sorted(set(per_signal[a]) & set(per_signal[b]))
                if len(shared) < 2:
                    continue
                rho = spearman(
                    [per_signal[a][asset] for asset in shared],
                    [per_signal[b][asset] for asset in shared],
                )
                if rho is not None:
                    values.append(rho)
            mean = sum(values) / len(values) if values else None
            out.append(PairCorrelation(a, b, mean, len(values)))
    return out


def ic_correlations(ic: pl.DataFrame) -> list[PairCorrelation]:
    """Correlation of the IC time series between every pair of signals.

    Args:
        ic: A `BacktestResult.ic` frame (`rebalance_ts`, `signal`, `ic`).

    Returns:
        One `PairCorrelation` per unordered pair, over their common rebalances.
    """
    if len(ic) == 0:
        return []

    series: dict[str, dict[datetime, float]] = {}
    for row in ic.iter_rows(named=True):
        if row["ic"] is None:
            continue
        series.setdefault(row["signal"], {})[row["rebalance_ts"]] = float(row["ic"])

    names = sorted(set(ic["signal"].to_list()))
    out: list[PairCorrelation] = []

    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            shared = sorted(set(series.get(a, {})) & set(series.get(b, {})))
            rho = (
                spearman([series[a][d] for d in shared], [series[b][d] for d in shared])
                if len(shared) >= 2
                else None
            )
            out.append(PairCorrelation(a, b, rho, len(shared)))
    return out


def correlation_frame(pairs: Sequence[PairCorrelation]) -> pl.DataFrame:
    """Pairwise correlations as a frame, sorted most-correlated first."""
    schema = {
        "signal_a": pl.Utf8,
        "signal_b": pl.Utf8,
        "correlation": pl.Float64,
        "n_observations": pl.Int64,
    }
    frame = pl.DataFrame(
        [
            {
                "signal_a": p.signal_a,
                "signal_b": p.signal_b,
                "correlation": p.correlation,
                "n_observations": p.n_observations,
            }
            for p in pairs
        ],
        schema=schema,
    )
    if len(frame) == 0:
        return frame
    return frame.sort(pl.col("correlation").abs(), descending=True, nulls_last=True)


def effective_breadth(pairs: Sequence[PairCorrelation], n_signals: int) -> float:
    """Independent-bet count implied by the average pairwise correlation.

    `n / (1 + (n - 1) * rho_bar)`, using the mean **absolute** correlation:
    two signals that reliably disagree are as redundant as two that agree, just
    with the sign flipped, so the magnitude is what costs breadth.

    Returns `float(n_signals)` when there is nothing to correlate (fewer than
    two signals, or no computable pair), and clamps a negative average to zero
    so the result never exceeds the signal count.
    """
    if n_signals <= 1:
        return float(max(n_signals, 0))

    values = [abs(p.correlation) for p in pairs if p.correlation is not None]
    if not values:
        return float(n_signals)

    rho_bar = max(0.0, sum(values) / len(values))
    return n_signals / (1.0 + (n_signals - 1) * rho_bar)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BreadthReport:
    """The breadth check's output.

    Attributes:
        signals: Signal ids covered.
        families: `{signal_id: family}`, when known — two signals in the same
            family being correlated is expected; across families it is a
            warning about the construction.
        score_pairs: Per-pair score correlations.
        ic_pairs: Per-pair IC correlations (empty if no IC frame was given).
        effective_breadth: Independent bets implied by `score_pairs`.
    """

    signals: list[str]
    families: dict[str, str] = field(default_factory=dict)
    score_pairs: list[PairCorrelation] = field(default_factory=list)
    ic_pairs: list[PairCorrelation] = field(default_factory=list)
    effective_breadth: float = 0.0

    def redundant_pairs(
        self, threshold: float = REDUNDANCY_THRESHOLD
    ) -> list[PairCorrelation]:
        """Pairs whose score correlation exceeds `threshold` in magnitude.

        Per the methodology template: keeping both then requires a written
        justification, or one of them should be retired.
        """
        return [
            p
            for p in self.score_pairs
            if p.correlation is not None and abs(p.correlation) > threshold
        ]

    def to_text(self, threshold: float = REDUNDANCY_THRESHOLD) -> str:
        """Human-readable summary, for scratch demos and the daily report."""
        lines = [
            f"Breadth report — {len(self.signals)} signals",
            f"Effective independent bets: {self.effective_breadth:.2f} "
            f"of {len(self.signals)}",
            "",
            f"{'pair':<58} {'score rho':>10} {'n':>6} {'IC rho':>8}",
            "-" * 86,
        ]
        ic_by_pair = {(p.signal_a, p.signal_b): p for p in self.ic_pairs}
        for pair in sorted(
            self.score_pairs,
            key=lambda p: -abs(p.correlation) if p.correlation is not None else 0.0,
        ):
            ic_pair = ic_by_pair.get((pair.signal_a, pair.signal_b))
            same_family = (
                self.families.get(pair.signal_a) == self.families.get(pair.signal_b)
                and pair.signal_a in self.families
            )
            marker = ""
            if pair.correlation is not None and abs(pair.correlation) > threshold:
                marker = "  <-- redundant?" if not same_family else "  <-- same family"
            lines.append(
                f"{pair.signal_a + ' / ' + pair.signal_b:<58} "
                f"{_fmt(pair.correlation):>10} {pair.n_observations:>6d} "
                f"{_fmt(ic_pair.correlation if ic_pair else None):>8}{marker}"
            )

        redundant = self.redundant_pairs(threshold)
        if redundant:
            lines += [
                "",
                f"{len(redundant)} pair(s) above |rho| = {threshold}: justify keeping "
                "both in each methodology doc, or retire one.",
            ]
        return "\n".join(lines)


def _fmt(value: float | None) -> str:
    return "     n/a" if value is None else f"{value:8.3f}"


def breadth_report(
    scores: pl.DataFrame,
    ic: pl.DataFrame | None = None,
    families: Mapping[str, str] | None = None,
) -> BreadthReport:
    """Build the breadth report from recorded scores and (optionally) an IC frame.

    Args:
        scores: Frame from `ScoreRecorder.frame()`.
        ic: `BacktestResult.ic`, to add the "do they work at the same times"
            view.
        families: `{signal_id: family}`, e.g.
            `{s.signal_id: s.family for s in all_signals().values()}`.

    Returns:
        A `BreadthReport`.
    """
    names = sorted(set(scores["signal"].to_list())) if len(scores) else []
    score_pairs = score_correlations(scores)
    ic_pairs = ic_correlations(ic) if ic is not None else []

    report = BreadthReport(
        signals=names,
        families=dict(families or {}),
        score_pairs=score_pairs,
        ic_pairs=ic_pairs,
        effective_breadth=effective_breadth(score_pairs, len(names)),
    )
    log.info(
        "Breadth: %d signals, %.2f effective independent bets, %d redundant pair(s)",
        len(names), report.effective_breadth, len(report.redundant_pairs()),
    )
    return report
