package com.zx.quant.klineproxy.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.client.ws.dispatch.KlineMessageDispatcher;
import com.zx.quant.klineproxy.client.ws.dispatch.WebSocketIngressLifecycle;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import com.zx.quant.klineproxy.util.Serializer;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.util.ReflectionTestUtils;

class WebSocketClientConfigTest {
  @Test
  void bindsIngressConfigurationAndInjectsOneSharedDispatcherIntoAllRegisteredClients() {
    try (var context = new AnnotationConfigApplicationContext()) {
      context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", Map.of(
          "kline.ingress.workers", "3",
          "kline.ingress.finalQueueCapacityPerWorker", "17",
          "kline.ingress.coalesceFormingUpdates", "false")));
      context.registerBean(Serializer.class, () -> new Serializer(new ObjectMapper()));
      context.registerBean(RateLimitManager.class, () -> mock(RateLimitManager.class));
      context.register(WebSocketClientConfig.class);
      context.refresh();
      var dispatcher = context.getBean(KlineMessageDispatcher.class);
      var properties = context.getBean(KlineIngressProperties.class);
      assertThat(dispatcher.workerCount()).isEqualTo(3);
      assertThat(properties.getFinalQueueCapacityPerWorker()).isEqualTo(17);
      assertThat(properties.isCoalesceFormingUpdates()).isFalse();
      var clients = context.getBeansOfType(WebSocketClient.class).values();
      assertThat(clients).hasSize(60);
      for (WebSocketClient client : clients) {
        assertThat(ReflectionTestUtils.getField(client, "klineMessageDispatcher")).isSameAs(dispatcher);
        assertThat(client.alive()).isFalse(); // no service started a network producer in this context
      }
      assertThat(context.getBean(WebSocketIngressLifecycle.class).isRunning()).isTrue();
    }
  }
}
