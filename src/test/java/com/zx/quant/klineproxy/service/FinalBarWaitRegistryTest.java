package com.zx.quant.klineproxy.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.zx.quant.klineproxy.service.FinalBarWaitRegistry.Key;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class FinalBarWaitRegistryTest {
  private final Key btc = new Key("BTCUSDT", "1h", 0);
  private final Key eth = new Key("ETHUSDT", "1h", 0);

  @Test
  void retainsNotificationBetweenStoreCheckAndAwaitWithoutWakingUnrelatedRequests() throws Exception {
    FinalBarWaitRegistry registry = new FinalBarWaitRegistry();
    try (var a = registry.register(List.of(btc)); var b = registry.register(List.of(eth));
        var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      long checked = a.version();
      registry.signal(new Key("BTCUSDT", "1h", 3_600_000));
      registry.signal(new Key("BTCUSDT", "1d", 0));
      assertThat(a.version()).isEqualTo(checked);
      registry.signal(btc); // final arrived after the caller's store check, before await
      executor.submit(() -> { a.awaitChange(checked, TimeUnit.SECONDS.toNanos(30)); return null; })
          .get(1, TimeUnit.SECONDS);
      assertThat(b.version()).isZero();
      assertThat(registry.subscribedKeyCount()).isEqualTo(1);
    }
    assertThat(registry.subscribedKeyCount()).isZero();
  }

  @Test
  void overlappingRequestsDetachIndependentlyAndRepeatedFinalsDoNotRetainKeys() {
    FinalBarWaitRegistry registry = new FinalBarWaitRegistry();
    try (var a = registry.register(List.of(btc, btc, eth)); var b = registry.register(List.of(btc))) {
      a.close();
      registry.signal(btc);
      assertThat(b.version()).isEqualTo(1);
      registry.signal(btc);
      assertThat(b.version()).isEqualTo(1);
      assertThat(registry.subscribedKeyCount()).isZero();
    }
  }

  @Test
  void timeoutAndInterruptionCanAlwaysRemoveTheirSubscriptions() throws Exception {
    FinalBarWaitRegistry registry = new FinalBarWaitRegistry();
    try (var registration = registry.register(List.of(btc, eth))) {
      registration.awaitChange(registration.version(), 1);
      Thread.currentThread().interrupt();
      assertThrows(InterruptedException.class,
          () -> registration.awaitChange(registration.version(), TimeUnit.SECONDS.toNanos(30)));
    } finally {
      Thread.interrupted();
    }
    assertThat(registry.subscribedKeyCount()).isZero();
  }

  @Test
  void concurrentPublishRegisterAndCancelLeaveNoListenerLeaks() throws Exception {
    FinalBarWaitRegistry registry = new FinalBarWaitRegistry();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = java.util.stream.IntStream.range(0, 500).mapToObj(i -> executor.submit(() -> {
        try (var registration = registry.register(List.of(btc, eth))) {
          registry.signal((i & 1) == 0 ? btc : eth);
        }
      })).toList();
      for (var future : futures) {
        future.get(2, TimeUnit.SECONDS);
      }
    }
    assertThat(registry.subscribedKeyCount()).isZero();
  }
}
