from __future__ import annotations

import pytest

from bist_backtest.features import compute_features
from bist_backtest.strategies import build_strategy, strategy_names
from tests.support.backtest import synthetic_frame

EXPECTED_STRATEGIES = {
    "opening-range-breakout",
    "vwap-mean-reversion",
    "intraday-momentum",
    "intraday-reversal",
    "sma-crossover",
    "rsi-pullback",
    "previous-day-breakout",
    "volume-breakout",
    "buy-and-hold",
}


def decisions(name: str, closes: list[float], **params: object) -> list[str]:
    frame = synthetic_frame(closes, start_minute=540)
    features = compute_features(frame)
    strategy, _ = build_strategy(name, params)  # type: ignore[arg-type]
    return [strategy.fn(features, i) for i in range(len(frame))]


def test_registry_contains_all_strategies() -> None:
    assert EXPECTED_STRATEGIES.issubset(set(strategy_names()))


def test_orb_enters_above_range_and_only_after_break() -> None:
    out = decisions("opening-range-breakout", [100.0] * 35 + [101.5] + [102.0] * 3)
    assert out[:30] == ["hold"] * 30
    assert any(d == "long" for d in out[30:])


def test_orb_does_not_enter_without_break() -> None:
    out = decisions("opening-range-breakout", [100.0] * 60)
    assert out == ["hold"] * 60


def test_opening_window_is_half_open_and_excludes_next_bar() -> None:
    frame = synthetic_frame([100.0] * 30 + [200.0], start_minute=540)
    features = compute_features(frame, opening_minutes=30)
    assert features.opening_range_high == pytest.approx(100.1)


def test_orb_exits_at_configured_minute_even_after_price_falls_back() -> None:
    frame = synthetic_frame([100.0, 100.0, 102.0, 101.0, 100.0, 100.0], start_minute=540)
    features = compute_features(frame, opening_minutes=2)
    strategy, _ = build_strategy(
        "opening-range-breakout", {"opening_minutes": 2, "exit_minutes": 545}
    )
    out = [strategy.fn(features, i) for i in range(len(frame))]
    assert out[2] == "long"
    assert out[5] == "exit"


def test_vwap_reverts_only_when_below_threshold() -> None:
    frame = synthetic_frame([100.0] * 60, start_minute=540)
    frame.loc[40:, "close"] = frame.loc[40:, "close"] * 0.99
    frame.loc[40:, "open"] = frame.loc[40:, "open"] * 0.99
    features = compute_features(frame)
    strategy, _ = build_strategy("vwap-mean-reversion", {"deviation_threshold": 0.005})
    out = [strategy.fn(features, i) for i in range(len(frame))]
    assert out[0] == "hold"
    assert "long" in out[40:]


def test_vwap_holds_when_close_near_vwap() -> None:
    out = decisions("vwap-mean-reversion", [100.0] * 60)
    assert out == ["hold"] * 60


def test_sma_crossover_cross_up_then_down() -> None:
    closes = (
        [100.0] * 25 + [101.0 + 0.2 * k for k in range(15)] + [104.0 - 0.2 * k for k in range(20)]
    )
    out = decisions("sma-crossover", closes)
    assert "long" in out
    assert "exit" in out


@pytest.mark.parametrize(
    ("name", "closes"),
    [
        ("intraday-momentum", [100.0 + i for i in range(40)]),
        ("intraday-reversal", [100.0 - i for i in range(40)]),
    ],
)
def test_first_window_strategies_wait_for_window_close(name: str, closes: list[float]) -> None:
    out = decisions(name, closes, window_minutes=30)
    assert out[:29] == ["hold"] * 29
    assert out[29] == "long"


def test_rsi_pullback_buys_oversold_exits_recovered() -> None:
    closes = [100.0] * 5 + [100.0 - k for k in range(30)] + [100.0 - 30 + k for k in range(40)]
    out = decisions("rsi-pullback", closes, rsi_period=14, rsi_below=30.0, exit_above=55.0)
    assert "long" in out


def test_previous_day_breakout_uses_previous_session() -> None:
    from bist_backtest.features import PreviousSession

    frame = synthetic_frame([100.0] * 50, start_minute=540)
    features = compute_features(
        frame, previous_session=PreviousSession(close=99.0, high=110.0, low=98.0, session_date="x")
    )
    strategy, _ = build_strategy("previous-day-breakout")
    out = [strategy.fn(features, i) for i in range(len(frame))]
    assert out == ["hold"] * 50  # 100 < 110, no breakout

    frame2 = synthetic_frame([120.0] * 50, start_minute=540)
    features2 = compute_features(
        frame2, previous_session=PreviousSession(close=99.0, high=110.0, low=98.0, session_date="x")
    )
    out2 = [strategy.fn(features2, i) for i in range(len(frame2))]
    assert all(d == "long" for d in out2[1:10])


def test_volume_breakout_requires_volume() -> None:
    frame = synthetic_frame([100.0] * 40 + [101.5] * 10, start_minute=540, volume=1000)
    frame.loc[40:, "volume"] = 5000
    features = compute_features(frame)
    strategy, _ = build_strategy("volume-breakout", {"volume_mult": 2.0})
    out = [strategy.fn(features, i) for i in range(len(frame))]
    assert "long" in out

    frame_low = synthetic_frame([100.0] * 50, start_minute=540, volume=1000)
    features_low = compute_features(frame_low)
    strategy_low, _ = build_strategy("volume-breakout", {"volume_mult": 2.0})
    out_low = [strategy_low.fn(features_low, i) for i in range(len(frame_low))]
    assert out_low == ["hold"] * 50


def test_unknown_params_rejected() -> None:
    with pytest.raises(ValueError):
        build_strategy("sma-crossover", {"bogus": 1})


def test_buy_and_hold_rejects_params() -> None:
    with pytest.raises(ValueError):
        build_strategy("buy-and-hold", {"opening_minutes": 5})
