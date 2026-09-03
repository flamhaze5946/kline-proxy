# RESULT — kline-proxy 1.7.14 bulk closed-bar finality (branch final-wait)

## Design
- Flag lives in `KlineSet.finalOpenTimes` (concurrent set beside `klineMap`; trimmed with the map).
- Sources of "final": websocket kline event with `x=true` (`updateStreamKline`), REST candles whose `closeTime <= now` (`updateKlines` → `updateKlinesInternal(finalizeClosed=true)`), restored rows (`restoreKlines`), synthetic fills (`fillKlines` output not present in the input list).
- Wait: `queryBulkKlines(closed_only=true)` computes `boundary = floor(now/interval)`, `justClosedOpenTime = boundary - interval`; `pending` = requested symbols whose bar for that open time exists and is not final; if within `kline.bulk.finalWaitMaxMs` of the boundary, block on `finalSignal` (notified by every final mark; 25 ms poll fallback; budget measured with `System.nanoTime`) until pending is empty or the cap (measured from the boundary) elapses.
- `closed_only=false`: no finality semantics (unchanged behaviour, cached as before).
- Cache: key includes the boundary; only `finalized` responses are cached; identical concurrent requests are single-flighted through `bulkKlinesInFlight` (CompletableFuture per key).

## Config
`kline.bulk.finalWaitEnabled` (true), `kline.bulk.finalWaitMaxMs` (8000, clamp 0..30000); ops: `server.tomcat.threads.max` 600, `server.tomcat.accept-count` 500 (dotted root keys in application.yaml; the VPS copy needs the same keys).

## Response
`finalized` (bool), `pending` (symbols still non-final when returned), `waited_ms`. Legacy fields unchanged; the kline array format unchanged.

## Logs
`BULK_FINAL_WAIT interval boundary requested pending_initial waited_ms pending_after=0` (INFO) / `BULK_FINAL_WAIT_CAP … pending=[…]` (WARN).

## Tests (AbstractKlineServiceFillKlinesTest)
streamBarIsFinalOnlyWhenBinanceSaysClosed; bulkBlocksUntilTheClosingUpdateArrives; symbolsWithoutTheJustClosedBarAreNotWaitedForAndRestBarsAreFinal; outsideTheSettlingWindowAndWithClosedOnlyFalseThereIsNoWait (+ closed_only=false cached); cacheKeyIncludesTheBoundary; syntheticFillsAreFinal; concurrentIdenticalRequestsShareOneWait. Full suite green offline (`./mvnw -o -q test`); one pre-existing 1.7.13 test fixed (server time moved outside HourBoundaryGuard's after-window).

## Residual risk
- A symbol whose `x=true` never arrives (WS gap, delisting mid-hour) delays callers until the cap (8 s from the boundary), then returns `finalized=false` with `pending`; the 5-minute REST sync repairs the bar later.
- ~300 fleet requests block simultaneously for 1–5 s at the boundary: thread pool raised to 600; requests are per-shard distinct so single-flight rarely applies to them.
- Measured before the fix: 30–34% of symbols non-final at T+1 s, 0% by T+5 s ⇒ expected fleet bar-arrival T+2–5 s instead of T+1 s.
