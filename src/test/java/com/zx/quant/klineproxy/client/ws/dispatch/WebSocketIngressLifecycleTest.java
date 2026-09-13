package com.zx.quant.klineproxy.client.ws.dispatch;

import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.support.GenericApplicationContext;

class WebSocketIngressLifecycleTest {
  @Test
  void springStopsProducerAndDrainsAcceptedFinalsBeforePersistenceDestruction() throws Exception {
    var properties = new KlineIngressProperties();
    properties.setWorkers(1);
    var dispatcher = new KlineMessageDispatcher(properties);
    WebSocketClient producer = mock(WebSocketClient.class);
    CountDownLatch occupied = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger stored = new AtomicInteger();
    AtomicInteger persisted = new AtomicInteger(-1);
    var metadata = new KlineDispatchMetadata(new KlineDispatchMetadata.Series("future", "BTCUSDT", "1h"),
        0, true, 1, 1L, "btcusdt@kline_1h");
    dispatcher.submit(metadata, 1, () -> {
      occupied.countDown();
      try {
        if (!release.await(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test producer was not closed");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      stored.incrementAndGet();
    });
    assertThat(occupied.await(2, TimeUnit.SECONDS)).isTrue();
    dispatcher.submit(metadata, 2, stored::incrementAndGet);
    doAnswer(invocation -> { release.countDown(); return null; }).when(producer).close();
    try (var context = new GenericApplicationContext()) {
      context.registerBean("persistence", DisposableBean.class,
          () -> () -> persisted.set(stored.get()));
      context.registerBean(WebSocketIngressLifecycle.class,
          () -> new WebSocketIngressLifecycle(List.of(producer), dispatcher));
      context.refresh();
    } finally {
      release.countDown();
      dispatcher.close();
    }
    verify(producer).close();
    assertThat(persisted.get()).isEqualTo(2);
    assertThat(dispatcher.snapshot().processedFinal()).isEqualTo(2);
    assertThat(dispatcher.snapshot().failures()).isZero();
  }
}
