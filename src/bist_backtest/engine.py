from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from .features import Features


@dataclass(frozen=True, slots=True)
class CostConfig:
    commission_bps: float = 10.0
    slippage_bps: float = 5.0
    half_spread_bps: float = 1.0
    position_fraction: float = 1.0
    allow_short: bool = False
    starting_equity: float = 100_000.0

    @property
    def friction(self) -> float:
        return (self.commission_bps + self.slippage_bps + self.half_spread_bps) / 10_000.0

    def entry_fill(self, reference: float, side: str) -> float:
        if side == "long":
            return float(reference) * (1.0 + self.friction)
        if side == "short":
            return float(reference) * (1.0 - self.friction)
        raise ValueError(f"unknown side {side!r}")

    def exit_fill(self, reference: float, side: str) -> float:
        if side == "long":
            return float(reference) * (1.0 - self.friction)
        if side == "short":
            return float(reference) * (1.0 + self.friction)
        raise ValueError(f"unknown side {side!r}")


@dataclass(frozen=True, slots=True)
class FillRecord:
    bar_index: int
    kind: str
    side: str
    quantity: int
    reference: float
    fill: float
    forced: bool = False


@dataclass(frozen=True, slots=True)
class Trade:
    symbol: str
    session_date: str
    side: str
    entry_bar_index: int
    exit_bar_index: int | None
    entry_time_utc: pd.Timestamp
    exit_time_utc: pd.Timestamp | None
    entry_fill: float
    exit_fill: float | None
    quantity: int
    pnl: float
    return_pct: float
    forced: bool


@dataclass(frozen=True, slots=True)
class BarLedgerRow:
    bar_index: int
    bar_open_utc: pd.Timestamp
    decision: str
    position: int
    mark_price: float
    cash: float
    equity: float


@dataclass(frozen=True, slots=True)
class SimulationResult:
    symbol: str
    session_date: str
    ledger: pd.DataFrame
    fills: pd.DataFrame
    trades: pd.DataFrame
    final_equity: float
    total_return: float
    forced_closes: int
    skipped_entries: int

    @property
    def trade_count(self) -> int:
        return len(self.trades)


class Simulator:
    """Deterministic single-symbol event loop with next-bar fills.

    Decisions are produced at the close of bar ``i`` from features known at that
    close. An entry is filled at the open of bar ``i + 1``. An optional initial
    decision represents an order placed before the session and fills at the first
    open. A position still open after the last bar is force-closed at the last close.
    """

    def __init__(
        self,
        features: Features,
        symbol: str,
        session_date: str,
        costs: CostConfig,
        *,
        decisions: list[str],
        initial_decision: str | None = None,
    ) -> None:
        if len(decisions) != len(features.frame):
            raise ValueError("one decision per bar is required")
        if initial_decision not in (None, "long", "short"):
            raise ValueError("initial_decision must be long, short, or None")
        self.features = features
        self.frame = features.frame.reset_index(drop=True)
        self.symbol = symbol
        self.session_date = session_date
        self.costs = costs
        self.decisions = decisions
        self._cash = costs.starting_equity
        self._quantity = 0
        self._side: str | None = None
        self._entry_fill = 0.0
        self._entry_bar_index: int | None = None
        self._pending = initial_decision
        self.trades: list[Trade] = []
        self.fills: list[FillRecord] = []

    @property
    def position_quantity(self) -> int:
        return self._quantity

    def run(self) -> SimulationResult:
        rows: list[BarLedgerRow] = []
        forced_closes = 0
        skipped_entries = 0
        n = len(self.frame)

        for i in range(n):
            bar = self.frame.iloc[i]
            close = float(bar["close"])
            open_price = float(bar["open"])
            is_last = i == n - 1

            if self._pending in ("long", "short") and self._side is None:
                self._enter(self._pending, open_price, i)
            elif self._pending == "exit" and self._side is not None:
                self._exit(open_price, i, forced=False)
            self._pending = None

            decision = self.decisions[i]
            if decision in ("long", "short") and self._side is None:
                if is_last:
                    skipped_entries += 1
                else:
                    self._pending = decision
            elif decision == "exit" and self._side is not None:
                if is_last:
                    forced_closes += 1
                    self._exit(close, i, forced=True)
                else:
                    self._pending = "exit"
            elif is_last and self._side is not None:
                forced_closes += 1
                self._exit(close, i, forced=True)

            equity = self.equity(close)
            rows.append(
                BarLedgerRow(
                    bar_index=i,
                    bar_open_utc=bar["bar_open_utc"],
                    decision=decision,
                    position=self._quantity,
                    mark_price=close,
                    cash=self._cash,
                    equity=equity,
                )
            )

        final_equity = float(rows[-1].equity)
        ledger = pd.DataFrame([asdict(row) for row in rows])
        trades_frame = (
            pd.DataFrame([asdict(t) for t in self.trades]) if self.trades else pd.DataFrame()
        )
        fills_frame = (
            pd.DataFrame([asdict(f) for f in self.fills]) if self.fills else pd.DataFrame()
        )
        return SimulationResult(
            symbol=self.symbol,
            session_date=self.session_date,
            ledger=ledger,
            fills=fills_frame,
            trades=trades_frame,
            final_equity=final_equity,
            total_return=final_equity / self.costs.starting_equity - 1.0,
            forced_closes=forced_closes,
            skipped_entries=skipped_entries,
        )

    def _enter(self, side: str, reference: float, bar_index: int) -> None:
        if side == "short" and not self.costs.allow_short:
            return
        fill = self.costs.entry_fill(reference, side)
        budget = self._cash * self.costs.position_fraction
        quantity = max(int(budget / fill), 0)
        if quantity <= 0:
            return
        if side == "long":
            self._cash -= quantity * fill
        else:
            self._cash += quantity * fill
        self._quantity = quantity
        self._side = side
        self._entry_fill = fill
        self._entry_bar_index = bar_index
        self.fills.append(
            FillRecord(
                bar_index=bar_index,
                kind="entry",
                side=side,
                quantity=quantity,
                reference=reference,
                fill=fill,
                forced=False,
            )
        )

    def _exit(self, reference: float, bar_index: int, *, forced: bool) -> None:
        assert self._side is not None and self._quantity > 0
        fill = self.costs.exit_fill(reference, self._side)
        quantity = self._quantity
        side = self._side
        if side == "long":
            self._cash += quantity * fill
        else:
            self._cash -= quantity * fill
        pnl = (
            (fill - self._entry_fill) * quantity
            if side == "long"
            else (self._entry_fill - fill) * quantity
        )
        self.fills.append(
            FillRecord(
                bar_index=bar_index,
                kind="exit",
                side=side,
                quantity=quantity,
                reference=reference,
                fill=fill,
                forced=forced,
            )
        )
        self.trades.append(
            Trade(
                symbol=self.symbol,
                session_date=self.session_date,
                side=side,
                entry_bar_index=self._entry_bar_index or 0,
                exit_bar_index=bar_index,
                entry_time_utc=self.frame.iloc[self._entry_bar_index or 0]["bar_open_utc"],
                exit_time_utc=self.frame.iloc[bar_index]["bar_open_utc"],
                entry_fill=self._entry_fill,
                exit_fill=fill,
                quantity=quantity,
                pnl=pnl,
                return_pct=pnl / max(self._entry_fill * quantity, 1e-12),
                forced=forced,
            )
        )
        self._quantity = 0
        self._side = None
        self._entry_fill = 0.0
        self._entry_bar_index = None

    def equity(self, mark: float) -> float:
        if self._side == "long":
            return self._cash + self._quantity * mark
        if self._side == "short":
            return self._cash - self._quantity * mark
        return self._cash
