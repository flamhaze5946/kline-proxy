package com.zx.quant.klineproxy.monitor;

import static org.assertj.core.api.Assertions.assertThat;

import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import com.zx.quant.klineproxy.client.ws.dispatch.KlineMessageDispatcher;
import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import io.prometheus.client.CollectorRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class KlineIngressMetricsTest {
  @Test
  void exposesFinalAdmissionProcessingAndFailureAsSeparateMetrics() {
    var dispatcher = new KlineMessageDispatcher(new KlineIngressProperties());
    CollectorRegistry registry = new CollectorRegistry();
    var metrics = new KlineIngressMetrics(dispatcher, registry);
    try {
      dispatcher.submit(new KlineDispatchMetadata(new KlineDispatchMetadata.Series("future", "BTCUSDT", "1h"),
          0, true, 10, 100L, "topic"), 1, () -> { });
      assertThat(dispatcher.shutdown(Duration.ofSeconds(2))).isTrue();
      assertThat(registry.getSampleValue("websocket_kline_ingress_received_total", new String[]{"closed"}, new String[]{"true"})).isEqualTo(1.0);
      assertThat(registry.getSampleValue("websocket_kline_ingress_processed_total", new String[]{"closed"}, new String[]{"true"})).isEqualTo(1.0);
      assertThat(registry.getSampleValue("websocket_kline_ingress_failures_total")).isEqualTo(0.0);
    } finally {
      metrics.unregister();
      dispatcher.close();
    }
    assertThat(registry.getSampleValue("websocket_kline_ingress_failures_total")).isNull();
  }
}
