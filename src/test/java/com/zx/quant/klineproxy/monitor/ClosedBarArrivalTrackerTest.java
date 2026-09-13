package com.zx.quant.klineproxy.monitor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ClosedBarArrivalTrackerTest {
  private static final long H = 3_600_000L;

  private static void close(ClosedBarArrivalTracker tracker, String symbol, long arrival) {
    tracker.record("test", symbol, "1h", 0L, H, arrival, H + 100);
  }

  @Test
  void concurrentBoundaryUsesConstantUniverseScansAndDeduplicatesArrivalCounts() throws Exception {
    Set<String> symbols = IntStream.range(0, 500).mapToObj(i -> "S" + i).collect(Collectors.toSet());
    AtomicInteger snapshots = new AtomicInteger();
    var tracker = new ClosedBarArrivalTracker((interval, open) -> {
      snapshots.incrementAndGet();
      return new ClosedBarArrivalTracker.Universe(symbols, symbols);
    });
    try (var pool = Executors.newFixedThreadPool(4)) {
      var tasks = symbols.stream().map(symbol -> pool.submit(() -> {
        close(tracker, symbol, 120);
        close(tracker, symbol, 200);
      })).toList();
      for (var task : tasks) {
        task.get();
      }
    }
    var summary = tracker.lastSummary("1h");
    assertThat(summary.expected()).isEqualTo(500);
    assertThat(summary.arrived()).isEqualTo(500);
    assertThat(summary.firstMs()).isEqualTo(120);
    assertThat(summary.maxMs()).isEqualTo(120);
    assertThat(summary.eventCount()).isEqualTo(500);
    assertThat(summary.incomplete()).isFalse();
    assertThat(snapshots.get()).isEqualTo(2);
  }

  @Test
  void completionRechecksNewlyCachedSymbolsBeforePublishing() {
    var universe = new AtomicReference<>(new ClosedBarArrivalTracker.Universe(null, Set.of("A", "B")));
    var tracker = new ClosedBarArrivalTracker((interval, open) -> universe.get());
    close(tracker, "A", 100);
    universe.set(new ClosedBarArrivalTracker.Universe(null, Set.of("A", "B", "C")));
    close(tracker, "B", 200);
    assertThat(tracker.lastSummary("1h")).isNull();
    close(tracker, "C", 300);
    assertThat(tracker.lastSummary("1h").arrived()).isEqualTo(3);
    assertThat(tracker.lastSummary("1h").lastSymbol()).isEqualTo("C");
  }

  @Test
  void maintenanceReconcilesDelistingWithoutWaitingThirtySeconds() {
    var universe = new AtomicReference<>(new ClosedBarArrivalTracker.Universe(Set.of("A", "B"), Set.of("A", "B")));
    var tracker = new ClosedBarArrivalTracker((interval, open) -> universe.get());
    close(tracker, "A", 100);
    universe.set(new ClosedBarArrivalTracker.Universe(Set.of("A"), Set.of("A", "B")));
    tracker.maintain("test", H + 5_000);
    assertThat(tracker.lastSummary("1h").expected()).isEqualTo(1);
    assertThat(tracker.lastSummary("1h").notTrading()).containsExactly("B");
    assertThat(tracker.lastSummary("1h").incomplete()).isFalse();
  }

  @Test
  void previouslyExcludedArrivalsAreAvailableWhenStatusChangesDuringAnOpenBoundary() {
    Set<String> withBar = Set.of("A", "B", "C");
    var universe = new AtomicReference<>(new ClosedBarArrivalTracker.Universe(Set.of("A", "C"), withBar));
    var tracker = new ClosedBarArrivalTracker((interval, open) -> universe.get());
    close(tracker, "A", 100);
    close(tracker, "B", 150);
    universe.set(new ClosedBarArrivalTracker.Universe(withBar, withBar));
    close(tracker, "C", 200);
    assertThat(tracker.lastSummary("1h").expected()).isEqualTo(3);
    assertThat(tracker.lastSummary("1h").arrived()).isEqualTo(3);
  }

  @Test
  void timeoutAndLatePreviousBoundaryCannotReplaceCurrentStatistics() {
    var tracker = new ClosedBarArrivalTracker((interval, open) ->
        new ClosedBarArrivalTracker.Universe(null, Set.of("A", "B")));
    close(tracker, "A", 100);
    tracker.maintain("test", H + 31_000);
    assertThat(tracker.lastSummary("1h").incomplete()).isTrue();
    assertThat(tracker.lastSummary("1h").pending()).containsExactly("B");
    tracker.record("test", "A", "1h", H, H, 200, 2 * H + 100);
    close(tracker, "B", H + 300);
    assertThat(tracker.lastSummary("1h")).isNull();
    tracker.record("test", "B", "1h", H, H, 300, 2 * H + 100);
    assertThat(tracker.lastSummary("1h").boundary()).isEqualTo(2 * H);
    assertThat(tracker.lastSummary("1h").maxMs()).isEqualTo(300);
  }
}
