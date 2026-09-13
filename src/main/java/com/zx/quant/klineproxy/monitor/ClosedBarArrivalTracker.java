package com.zx.quant.klineproxy.monitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Boundary-level arrival accounting. The hot path removes one symbol from a pending set; it does
 * not rescan the cache or rebuild the trading universe for every closing update. Universe changes
 * are reconciled before declaring completion and by maintenance while a boundary is pending.
 * Statistics never decide whether a market-data update may be committed.
 */
@Slf4j
public final class ClosedBarArrivalTracker {
  private static final long INCOMPLETE_AFTER_MS = 30_000L;
  private static final int SYMBOLS_SHOWN = 20;
  private final UniverseProvider universeProvider;
  private final ConcurrentHashMap<String, Boundary> boundaries = new ConcurrentHashMap<>();

  public ClosedBarArrivalTracker(UniverseProvider universeProvider) {
    this.universeProvider = universeProvider;
  }

  @FunctionalInterface
  public interface UniverseProvider {
    Universe snapshot(String interval, long openTime);
  }

  /** null trading means exchange status is unknown, so every cached symbol is eligible. */
  public record Universe(Set<String> trading, Set<String> withBar) {
    public Universe {
      trading = trading == null ? null : Set.copyOf(trading);
      withBar = Set.copyOf(withBar);
    }

    private Set<String> expected() {
      Set<String> expected = new HashSet<>(withBar);
      if (trading != null) {
        expected.retainAll(trading);
      }
      return expected;
    }

    private List<String> notTrading() {
      return trading == null ? List.of()
          : withBar.stream().filter(symbol -> !trading.contains(symbol)).sorted().toList();
    }
  }

  public void record(String service, String symbol, String interval, long openTime, long intervalMs,
      long arrivalMs, Long eventTimeMs) {
    long boundaryTime = openTime + intervalMs;
    Boundary state = boundaries.get(interval);
    if (state == null || state.boundary < boundaryTime) {
      state = boundaries.compute(interval, (key, current) -> {
        if (current == null || current.boundary < boundaryTime) {
          if (current != null) {
            report(service, finish(current, true));
          }
          return new Boundary(interval, openTime, boundaryTime,
              universeProvider.snapshot(interval, openTime).expected());
        }
        return current;
      });
    }
    if (state.boundary != boundaryTime || state.summary != null) {
      return; // late/duplicate statistics never prevent the caller from storing a final update
    }
    boolean candidate;
    synchronized (state) {
      if (state.summary != null) {
        return;
      }
      state.arrivals.putIfAbsent(symbol, arrivalMs);
      if (eventTimeMs != null) {
        state.eventOffsets.putIfAbsent(symbol, eventTimeMs - boundaryTime);
      }
      state.pending.remove(symbol);
      candidate = state.pending.isEmpty() && state.expected.contains(symbol);
    }
    if (candidate) {
      report(service, finish(state, false));
    }
  }

  /** Called by existing maintenance, including before the 30-second incomplete deadline. */
  public void maintain(String service, long now) {
    for (Boundary state : boundaries.values()) {
      if (state.summary == null) {
        report(service, finish(state, now - state.boundary >= INCOMPLETE_AFTER_MS));
      }
    }
  }

  public Summary lastSummary(String interval) {
    Boundary state = boundaries.get(interval);
    return state == null ? null : state.summary;
  }

  private Summary finish(Boundary state, boolean timeout) {
    synchronized (state) {
      if (state.summary != null) {
        return null;
      }
      Universe universe = universeProvider.snapshot(state.interval, state.openTime);
      state.expected = universe.expected();
      state.pending = new HashSet<>(state.expected);
      state.pending.removeAll(state.arrivals.keySet());
      if (!timeout && !state.pending.isEmpty()) {
        return null;
      }
      List<Map.Entry<String, Long>> arrivals = state.arrivals.entrySet().stream()
          .filter(entry -> state.expected.contains(entry.getKey()))
          .sorted(Map.Entry.comparingByValue()).toList();
      List<Long> events = state.eventOffsets.entrySet().stream()
          .filter(entry -> state.expected.contains(entry.getKey()))
          .map(Map.Entry::getValue).sorted().toList();
      List<Long> offsets = arrivals.stream().map(Map.Entry::getValue).toList();
      List<String> pending = state.pending.stream().sorted().toList();
      state.summary = new Summary(state.interval, state.boundary, state.expected.size(), arrivals.size(),
          percentile(offsets, 0), percentile(offsets, .5), percentile(offsets, .9), percentile(offsets, 1),
          arrivals.isEmpty() ? "-" : arrivals.getLast().getKey(), !pending.isEmpty(), pending,
          universe.notTrading(), events.size(), percentile(events, 0), percentile(events, .5),
          percentile(events, .9), percentile(events, 1));
      return state.summary;
    }
  }

  private static long percentile(List<Long> sorted, double quantile) {
    return sorted.isEmpty() ? -1 : sorted.get(Math.min(sorted.size() - 1, (int) (sorted.size() * quantile)));
  }

  private static List<String> shown(List<String> symbols) {
    return symbols.size() <= SYMBOLS_SHOWN ? symbols : symbols.subList(0, SYMBOLS_SHOWN);
  }

  private static void report(String service, Summary s) {
    if (s == null) {
      return;
    }
    if (s.incomplete) {
      log.warn("CLOSED_BAR_SETTLE_INCOMPLETE interval={} boundary={} expected={} arrived={} first_ms={} p50_ms={} p90_ms={} max_ms={} pending={} pending_symbols={} not_trading={} not_trading_symbols={} ev_n={} ev_first_ms={} ev_p50_ms={} ev_p90_ms={} ev_max_ms={} service={}",
          s.interval, s.boundary, s.expected, s.arrived, s.firstMs, s.p50Ms, s.p90Ms, s.maxMs,
          s.pending.size(), shown(s.pending), s.notTrading.size(), shown(s.notTrading),
          s.eventCount, s.eventFirstMs, s.eventP50Ms, s.eventP90Ms, s.eventMaxMs, service);
    } else {
      log.info("CLOSED_BAR_SETTLED interval={} boundary={} expected={} arrived={} first_ms={} p50_ms={} p90_ms={} max_ms={} last={} not_trading={} not_trading_symbols={} ev_n={} ev_first_ms={} ev_p50_ms={} ev_p90_ms={} ev_max_ms={} service={}",
          s.interval, s.boundary, s.expected, s.arrived, s.firstMs, s.p50Ms, s.p90Ms, s.maxMs, s.lastSymbol,
          s.notTrading.size(), shown(s.notTrading), s.eventCount, s.eventFirstMs, s.eventP50Ms,
          s.eventP90Ms, s.eventMaxMs, service);
    }
  }

  public record Summary(String interval, long boundary, int expected, int arrived, long firstMs,
      long p50Ms, long p90Ms, long maxMs, String lastSymbol, boolean incomplete, List<String> pending,
      List<String> notTrading, int eventCount, long eventFirstMs, long eventP50Ms, long eventP90Ms,
      long eventMaxMs) { }

  private static final class Boundary {
    private final String interval;
    private final long openTime;
    private final long boundary;
    private final Map<String, Long> arrivals = new HashMap<>();
    private final Map<String, Long> eventOffsets = new HashMap<>();
    private Set<String> expected;
    private Set<String> pending;
    private volatile Summary summary;

    private Boundary(String interval, long openTime, long boundary, Set<String> expected) {
      this.interval = interval;
      this.openTime = openTime;
      this.boundary = boundary;
      this.expected = expected;
      this.pending = new HashSet<>(expected);
    }
  }
}
