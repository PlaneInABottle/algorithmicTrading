from __future__ import annotations

import argparse
import asyncio
import json
import signal
import time
from datetime import timedelta
from pathlib import Path

from bist_minute_collector.collector import Collector, CollectorConfig, SymbolRoute
from bist_minute_collector.models import FetchResult, RequestWindow
from bist_minute_collector.store import CursorUpdate, IngestEnvelope, WriterStore
from tests.helpers import BASE_OPEN, fetch_result


class BlockingProvider:
    source = "yahoo-chart"

    def __init__(self, ready: Path) -> None:
        self.ready = ready

    async def fetch_one(self, symbol: str, window: RequestWindow) -> FetchResult:
        del symbol, window
        self.ready.write_text("fetch-started", encoding="utf-8")
        await asyncio.Event().wait()
        raise AssertionError("blocking provider unexpectedly returned")


def hold_lock(data_root: Path, ready: Path) -> None:
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    with WriterStore(data_root):
        ready.write_text("lock-held", encoding="utf-8")
        while not stopping:
            time.sleep(0.01)


def killed_transaction(data_root: Path, ready: Path) -> None:
    def pause(point: str) -> None:
        if point == "before_commit":
            ready.write_text("transaction-open", encoding="utf-8")
            while True:
                time.sleep(1)

    result = fetch_result()
    with WriterStore(data_root, fault_injector=pause) as store:
        store.ingest(
            IngestEnvelope(
                fetch_id="killed-before-commit",
                exchange_code="ASELS.E",
                result=result,
                retained_response=b'{"subprocess":"sigkill"}',
                cursor=CursorUpdate(
                    accounted_through_utc=BASE_OPEN + timedelta(hours=1),
                    last_complete_bar_open_utc=BASE_OPEN,
                ),
            )
        )


def signal_collector(data_root: Path, ready: Path, stopped: Path) -> None:
    async def run() -> None:
        loop = asyncio.get_running_loop()
        with WriterStore(data_root) as store:
            collector = Collector(
                BlockingProvider(ready),
                store,
                (SymbolRoute("ASELS.E", "ASELS.IS"),),
                config=CollectorConfig(request_timeout_seconds=30),
            )
            task = asyncio.create_task(collector.collect_once())
            stop_requested = asyncio.Event()

            def request_stop() -> None:
                stop_requested.set()

            loop.add_signal_handler(signal.SIGTERM, request_stop)
            await stop_requested.wait()
            await collector.shutdown(timeout_seconds=2)
            report = await task
            stopped.write_text(
                json.dumps(
                    {
                        "cancelled_requests": report.cancelled_requests,
                        "quick_check": store.quick_check(),
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )

    asyncio.run(run())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("hold-lock", "killed-transaction", "signal-collector"))
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--ready", type=Path, required=True)
    parser.add_argument("--stopped", type=Path)
    args = parser.parse_args()
    if args.mode == "hold-lock":
        hold_lock(args.data_root, args.ready)
    elif args.mode == "killed-transaction":
        killed_transaction(args.data_root, args.ready)
    else:
        if args.stopped is None:
            raise SystemExit("--stopped is required for signal-collector")
        signal_collector(args.data_root, args.ready, args.stopped)


if __name__ == "__main__":
    main()
