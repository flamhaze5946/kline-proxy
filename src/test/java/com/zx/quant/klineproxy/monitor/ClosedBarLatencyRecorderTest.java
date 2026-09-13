package com.zx.quant.klineproxy.monitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.zx.quant.klineproxy.model.WebSocketMessageTiming;
import com.zx.quant.klineproxy.monitor.ClosedBarLatencyRecorder.Sample;
import com.zx.quant.klineproxy.monitor.ClosedBarLatencyRecorder.Trace;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;

class ClosedBarLatencyRecorderTest {

  private static final long BOUNDARY = 1_789_304_400_000L;

  @Test
  void decomposesPairedMessageWithoutMixingExchangeTimeQueueTimeOrPostFinalWork() {
    var metrics = sample("ADAUSDT", BOUNDARY + 106).metrics(BOUNDARY);
    assertThat(metrics.get("event_offset_ms")).isEqualTo(106.0);
    assertThat(metrics.get("receive_offset_ms")).isEqualTo(132.0);
    assertThat(metrics.get("event_to_receive_ms")).isEqualTo(26.0);
    assertThat(metrics.get("queue_ms")).isEqualTo(850.0);
    assertThat(metrics.get("settle_ms")).isEqualTo(200.0);
    assertThat(metrics.get("ready_offset_ms")).isEqualTo(998.0);
    assertThat(metrics.get("done_offset_ms")).isEqualTo(1204.0);
    double stages = List.of("frame_to_enqueue_ms", "queue_ms", "json_ms", "dispatch_ms",
        "decode_ms", "cache_ms", "finalize_ms", "settle_ms", "monitor_ms")
        .stream().mapToDouble(metrics::get).sum();
    assertThat(106.0 + metrics.get("event_to_receive_ms") + stages).isCloseTo(1204.0, within(0.000001));
  }

  @Test
  void keepsFirstSampleDeduplicatesAndFlushesIntervalsIndependentlyOnlyOnce() {
    var recorder = new ClosedBarLatencyRecorder();
    Sample first = sample("ADAUSDT", BOUNDARY + 106);
    recorder.record("1h", BOUNDARY, first);
    recorder.record("1h", BOUNDARY, sample("ADAUSDT", BOUNDARY + 200));
    recorder.record("1d", BOUNDARY, first);
    assertThat(recorder.drainDue(BOUNDARY + 29_999)).isEmpty();
    var snapshots = recorder.drainDue(BOUNDARY + 30_000);
    assertThat(snapshots).hasSize(2);
    snapshots.forEach(snapshot -> assertThat(snapshot.samples()).containsExactly(first));
    recorder.record("1h", BOUNDARY, sample("BTCUSDT", BOUNDARY + 106));
    assertThat(recorder.drainDue(BOUNDARY + 35_000)).isEmpty();
    long next = BOUNDARY + 3_600_000;
    recorder.record("1h", next, first);
    recorder.record("1h", BOUNDARY, sample("OLDUSDT", BOUNDARY + 106));
    var nextSnapshots = recorder.drainDue(next + 30_000);
    assertThat(nextSnapshots).hasSize(1);
    assertThat(nextSnapshots.getFirst().boundary()).isEqualTo(next);
    assertThat(nextSnapshots.getFirst().samples()).containsExactly(first);
  }

  @Test
  void boundsMemoryAndReportsSampleOverflow() {
    var recorder = new ClosedBarLatencyRecorder();
    for (int i = 0; i <= ClosedBarLatencyRecorder.MAX_SYMBOLS; i++) {
      recorder.record("1h", BOUNDARY, sample("COIN" + i, BOUNDARY + 106));
    }
    var snapshot = recorder.drainDue(BOUNDARY + 30_000).getFirst();
    assertThat(snapshot.samples()).hasSize(ClosedBarLatencyRecorder.MAX_SYMBOLS);
    assertThat(snapshot.overflow()).isEqualTo(1);
  }

  @Test
  void missingEventTimeIsExcludedAndLoggerStillIncludesLaterValidEventSamples() {
    var recorder = new ClosedBarLatencyRecorder();
    recorder.record("1h", BOUNDARY, sample("MISSINGUSDT", null));
    recorder.record("1h", BOUNDARY, sample("VALIDUSDT", BOUNDARY + 106));
    assertThat(sample("MISSINGUSDT", null).metrics(BOUNDARY))
        .doesNotContainKeys("event_offset_ms", "event_to_receive_ms");
    Logger logger = (Logger) LoggerFactory.getLogger(ClosedBarLatencyRecorder.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      recorder.logDue("binance-future", BOUNDARY + 30_000);
      String summary = appender.list.stream().map(ILoggingEvent::getFormattedMessage)
          .filter(line -> line.startsWith("CLOSED_BAR_LATENCY ")).findFirst().orElseThrow();
      assertThat(summary).contains("service=binance-future", "n=2 ev_n=1", "event_offset_ms_max=106.0",
          "queue_ms_p50=850.0", "ready_offset_ms_max=998.0", "done_offset_ms_max=1204.0");
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
  }

  private static Sample sample(String symbol, Long eventTime) {
    // Negative nanoTime values are legal. A -10 ms server offset corrects the wall clock.
    long base = -9_000_000_000L;
    var transport = new WebSocketMessageTiming("future-1", BOUNDARY + 142, base);
    ReflectionTestUtils.setField(transport, "enqueuedAtNanos", base + 1_000_000L);
    ReflectionTestUtils.setField(transport, "handlerStartedAtNanos", base + 851_000_000L);
    ReflectionTestUtils.setField(transport, "jsonParsedAtNanos", base + 852_000_000L);
    transport.executorSnapshot(4, 4, 0);
    Trace trace = new Trace(transport);
    ReflectionTestUtils.setField(trace, "decodeStarted", base + 854_000_000L);
    ReflectionTestUtils.setField(trace, "cacheStarted", base + 857_000_000L);
    ReflectionTestUtils.setField(trace, "cacheUpdated", base + 861_000_000L);
    ReflectionTestUtils.setField(trace, "finalized", base + 866_000_000L);
    ReflectionTestUtils.setField(trace, "settleRecorded", base + 1_066_000_000L);
    ReflectionTestUtils.setField(trace, "completed", base + 1_072_000_000L);
    return new Sample(symbol, "continuous_kline", eventTime, -10, 0, trace);
  }
}
