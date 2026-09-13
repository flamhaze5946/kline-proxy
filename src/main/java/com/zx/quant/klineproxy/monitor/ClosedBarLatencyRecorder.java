package com.zx.quant.klineproxy.monitor;

import com.zx.quant.klineproxy.model.WebSocketMessageTiming;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Bounded, per-service closing-message diagnostics. Collection never formats or writes logs.
 * The existing five-second maintenance task flushes each boundary at/after +30 seconds.
 * Samples count first processed x=true messages (including non-TRADING symbols), not REST finals
 * or an expected universe. Late messages after flush and older boundaries are not sampled.
 */
@Slf4j
public final class ClosedBarLatencyRecorder {

  static final long FLUSH_AFTER_MS = 30_000;
  static final int MAX_SYMBOLS = 4096;
  private final Map<String, Window> windows = new ConcurrentHashMap<>();

  public void record(String interval, long boundary, Sample sample) {
    Window window = windows.get(interval);
    if (window == null || window.boundary < boundary) {
      window = windows.compute(interval, (key, current) ->
          current == null || current.boundary < boundary ? new Window(boundary) : current);
    }
    if (window.boundary == boundary) {
      window.add(sample);
    }
  }

  public void logDue(String service, long now) {
    for (Snapshot snapshot : drainDue(now)) {
      List<Map<String, Double>> values = snapshot.samples.stream()
          .map(sample -> sample.metrics(snapshot.boundary)).toList();
      StringJoiner stats = new StringJoiner(" ");
      for (String key : values.stream().flatMap(value -> value.keySet().stream()).distinct().toList()) {
        List<Double> sorted = values.stream().filter(value -> value.containsKey(key))
            .map(value -> value.get(key)).sorted().toList();
        stats.add(key + "_p50=" + rounded(percentile(sorted, 0.5)))
            .add(key + "_p90=" + rounded(percentile(sorted, 0.9)))
            .add(key + "_max=" + rounded(sorted.getLast()));
      }
      // E can be absent on any sample; it must not be reported as a fabricated zero.
      long eventCount = snapshot.samples.stream().filter(sample -> sample.eventTime != null).count();
      log.info("CLOSED_BAR_LATENCY service={} interval={} boundary={} n={} ev_n={} overflow={} flush_offset_ms={} {}",
          service, snapshot.interval, snapshot.boundary, snapshot.samples.size(), eventCount,
          snapshot.overflow, now - snapshot.boundary, stats);

      Map<String, List<Sample>> clients = snapshot.samples.stream()
          .collect(Collectors.groupingBy(sample -> sample.trace.transport.getClient()));
      clients.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
        List<Map<String, Double>> clientValues = entry.getValue().stream()
            .map(sample -> sample.metrics(snapshot.boundary)).toList();
        StringJoiner clientStats = new StringJoiner(" ");
        for (String key : List.of("receive_offset_ms", "queue_ms", "ready_offset_ms", "queued_at_receive", "pool_size")) {
          List<Double> sorted = clientValues.stream().map(value -> value.get(key)).sorted().toList();
          clientStats.add(key + "_p50=" + rounded(percentile(sorted, 0.5)))
              .add(key + "_max=" + rounded(sorted.getLast()));
        }
        log.info("CLOSED_BAR_LATENCY_CLIENT service={} interval={} boundary={} client={} n={} {}",
            service, snapshot.interval, snapshot.boundary, entry.getKey(), entry.getValue().size(), clientStats);
      });

      // Include both the last available bars and the most expensive worker tasks, at most ten.
      LinkedHashSet<Sample> details = new LinkedHashSet<>();
      snapshot.samples.stream().sorted(Comparator.comparingDouble(
          (Sample sample) -> sample.readyOffset(snapshot.boundary)).reversed()).limit(5).forEach(details::add);
      snapshot.samples.stream().sorted(Comparator.comparingDouble(
          Sample::processingMillis).reversed()).limit(5).forEach(details::add);
      for (Sample sample : details) {
        StringJoiner detail = new StringJoiner(" ");
        sample.metrics(snapshot.boundary).forEach((key, value) -> detail.add(key + "=" + rounded(value)));
        log.info("CLOSED_BAR_LATENCY_DETAIL service={} interval={} boundary={} symbol={} event={} client={} received_wall_ms={} {}",
            service, snapshot.interval, snapshot.boundary, sample.symbol, sample.eventType,
            sample.trace.transport.getClient(), sample.trace.transport.getReceivedAtMillis(), detail);
      }
    }
  }

  List<Snapshot> drainDue(long now) {
    List<Snapshot> result = new ArrayList<>();
    windows.forEach((interval, window) -> {
      if (now - window.boundary >= FLUSH_AFTER_MS) {
        Snapshot snapshot = window.drain(interval);
        if (snapshot != null) {
          result.add(snapshot);
        }
      }
    });
    return result;
  }

  private static double percentile(List<Double> sorted, double quantile) {
    return sorted.get(Math.min(sorted.size() - 1, (int) Math.floor(sorted.size() * quantile)));
  }

  private static double rounded(double value) {
    return Math.round(value * 1000.0) / 1000.0;
  }

  private static double millis(long end, long start) {
    return (end - start) / 1_000_000.0;
  }

  /** Owned exclusively by the worker until record(); no additional clocks on forming bars. */
  public static final class Trace {
    private final WebSocketMessageTiming transport;
    private final long decodeStarted;
    private long cacheStarted;
    private long cacheUpdated;
    private long finalized;
    private long settleRecorded;
    private long completed;

    public Trace(WebSocketMessageTiming transport) {
      this.transport = transport;
      this.decodeStarted = System.nanoTime();
    }

    public void cacheStarting() { cacheStarted = System.nanoTime(); }
    public void cacheUpdated() { cacheUpdated = System.nanoTime(); }
    public void finalized() { finalized = System.nanoTime(); }
    public void settleRecorded() { settleRecorded = System.nanoTime(); }
    public void completed() { completed = System.nanoTime(); }
  }

  public record Sample(String symbol, String eventType, Long eventTime, long clockOffsetMs,
                       long clockSampleSpanMs, Trace trace) {

    private double receiveOffset(long boundary) {
      return (double) (trace.transport.getReceivedAtMillis() + clockOffsetMs - boundary);
    }

    private double readyOffset(long boundary) {
      return receiveOffset(boundary) + millis(trace.finalized, trace.transport.getReceivedAtNanos());
    }

    private double processingMillis() {
      return millis(trace.completed, trace.transport.getHandlerStartedAtNanos());
    }

    Map<String, Double> metrics(long boundary) {
      WebSocketMessageTiming transport = trace.transport;
      double receiveOffset = receiveOffset(boundary);
      Map<String, Double> out = new LinkedHashMap<>();
      // Exchange E is an event timestamp, not a guaranteed send time. This gap includes transport,
      // OS/Netty scheduling, frame assembly and clock-estimation error; it is not network RTT.
      if (eventTime != null) {
        out.put("event_offset_ms", (double) (eventTime - boundary));
        out.put("event_to_receive_ms", receiveOffset - (eventTime - boundary));
      }
      out.put("receive_offset_ms", receiveOffset);
      out.put("frame_to_enqueue_ms", millis(transport.getEnqueuedAtNanos(), transport.getReceivedAtNanos()));
      out.put("queue_ms", millis(transport.getHandlerStartedAtNanos(), transport.getEnqueuedAtNanos()));
      out.put("json_ms", millis(transport.getJsonParsedAtNanos(), transport.getHandlerStartedAtNanos()));
      out.put("dispatch_ms", millis(trace.decodeStarted, transport.getJsonParsedAtNanos()));
      out.put("decode_ms", millis(trace.cacheStarted, trace.decodeStarted));
      out.put("cache_ms", millis(trace.cacheUpdated, trace.cacheStarted));
      out.put("finalize_ms", millis(trace.finalized, trace.cacheUpdated));
      out.put("settle_ms", millis(trace.settleRecorded, trace.finalized));
      out.put("monitor_ms", millis(trace.completed, trace.settleRecorded));
      out.put("processing_ms", processingMillis());
      out.put("ready_offset_ms", readyOffset(boundary));
      out.put("done_offset_ms", receiveOffset + millis(trace.completed, transport.getReceivedAtNanos()));
      out.put("clock_offset_ms", (double) clockOffsetMs);
      out.put("clock_sample_span_ms", (double) clockSampleSpanMs);
      out.put("queued_at_receive", (double) transport.getQueuedAtReceive());
      out.put("queued_at_start", (double) transport.getQueuedAtStart());
      out.put("active_threads", (double) transport.getActiveThreads());
      out.put("pool_size", (double) transport.getPoolSize());
      out.put("dropped_total", (double) transport.getDroppedMessages());
      return out;
    }
  }

  record Snapshot(String interval, long boundary, List<Sample> samples, int overflow) { }

  private static final class Window {
    private final long boundary;
    private final Map<String, Sample> samples = new LinkedHashMap<>();
    private boolean flushed;
    private int overflow;

    private Window(long boundary) { this.boundary = boundary; }

    private synchronized void add(Sample sample) {
      if (flushed || samples.containsKey(sample.symbol)) {
        return;
      }
      if (samples.size() >= MAX_SYMBOLS) {
        overflow++;
      } else {
        samples.put(sample.symbol, sample);
      }
    }

    private synchronized Snapshot drain(String interval) {
      if (flushed || samples.isEmpty()) {
        return null;
      }
      flushed = true;
      Snapshot snapshot = new Snapshot(interval, boundary, List.copyOf(samples.values()), overflow);
      samples.clear();
      return snapshot;
    }
  }
}
