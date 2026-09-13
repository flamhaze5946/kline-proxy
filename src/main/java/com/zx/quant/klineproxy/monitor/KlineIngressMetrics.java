package com.zx.quant.klineproxy.monitor;

import com.zx.quant.klineproxy.client.ws.dispatch.KlineMessageDispatcher;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.CounterMetricFamily;
import io.prometheus.client.GaugeMetricFamily;
import jakarta.annotation.PreDestroy;
import java.util.List;
import org.springframework.stereotype.Component;

/** Separate admitted, attempted, coalesced and failed work; coalescing is not a dropped final. */
@Component
public final class KlineIngressMetrics extends Collector {
  private final KlineMessageDispatcher dispatcher;
  private final CollectorRegistry registry;

  public KlineIngressMetrics(KlineMessageDispatcher dispatcher, CollectorRegistry registry) {
    this.dispatcher = dispatcher;
    this.registry = registry;
    registry.register(this);
  }

  @Override
  public List<MetricFamilySamples> collect() {
    var snapshot = dispatcher.snapshot();
    CounterMetricFamily received = new CounterMetricFamily("websocket_kline_ingress_received_total",
        "Classified kline tasks admitted, including producers waiting for capacity", List.of("closed"));
    received.addMetric(List.of("true"), snapshot.receivedFinal());
    received.addMetric(List.of("false"), snapshot.receivedForming());
    CounterMetricFamily processed = new CounterMetricFamily("websocket_kline_ingress_processed_total",
        "Completed handler attempts; inspect failures separately", List.of("closed"));
    processed.addMetric(List.of("true"), snapshot.processedFinal());
    processed.addMetric(List.of("false"), snapshot.processedForming());
    CounterMetricFamily merged = new CounterMetricFamily("websocket_kline_ingress_coalesced_total",
        "Forming snapshots superseded before full processing", List.of("reason"));
    merged.addMetric(List.of("superseded"), snapshot.coalescedForming());
    merged.addMetric(List.of("stale"), snapshot.staleForming());
    GaugeMetricFamily queue = new GaugeMetricFamily("websocket_kline_ingress_queue",
        "Queued tasks, excluding running or capacity-blocked producers", List.of("closed"));
    queue.addMetric(List.of("true"), snapshot.queuedFinal());
    queue.addMetric(List.of("false"), snapshot.queuedForming());
    return List.of(received, processed, merged, queue,
        new CounterMetricFamily("websocket_kline_ingress_failures_total", "Failed handler attempts", snapshot.failures()),
        new CounterMetricFamily("websocket_kline_ingress_backpressure_total", "Producer capacity waits", snapshot.backpressureCount()),
        new CounterMetricFamily("websocket_kline_ingress_backpressure_seconds_total", "Producer capacity wait seconds",
            snapshot.backpressureNanos() / 1_000_000_000.0),
        new GaugeMetricFamily("websocket_kline_ingress_active_workers", "Active kline workers", snapshot.activeWorkers()));
  }

  @PreDestroy
  public void unregister() {
    registry.unregister(this);
  }
}
