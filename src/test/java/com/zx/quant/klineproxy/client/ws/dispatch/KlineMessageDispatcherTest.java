package com.zx.quant.klineproxy.client.ws.dispatch;

import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

class KlineMessageDispatcherTest {
  private static KlineDispatchMetadata data(String symbol, long open, boolean closed, int n, long event) {
    return new KlineDispatchMetadata(new KlineDispatchMetadata.Series("future", symbol, "1h"),
        open, closed, n, event, "topic");
  }

  private static KlineIngressProperties config(int finals, int forming) {
    KlineIngressProperties config = new KlineIngressProperties();
    config.setWorkers(1);
    config.setFinalQueueCapacityPerWorker(finals);
    config.setFormingQueueCapacityPerWorker(forming);
    return config;
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("latch timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static boolean eventually(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(1);
    }
    return condition.getAsBoolean();
  }

  @Test
  void prioritizesEveryFinalAndKeepsLatestFormingForEachOpenTime() {
    var dispatcher = new KlineMessageDispatcher(config(20, 20));
    CountDownLatch running = new CountDownLatch(1), release = new CountDownLatch(1);
    List<String> handled = new CopyOnWriteArrayList<>();
    try {
      dispatcher.submit(data("BLOCK", 0, false, 1, 1), 1, () -> { running.countDown(); await(release); });
      await(running);
      for (int i = 1; i <= 100; i++) {
        int n = i;
        dispatcher.submit(data("A", 0, false, n, n), n, () -> handled.add("old-" + n));
        dispatcher.submit(data("A", 3_600_000, false, n, n), n, () -> handled.add("new-" + n));
      }
      // A late old version must not replace the newer pending forming snapshot.
      dispatcher.submit(data("A", 3_600_000, false, 50, 200), 200, () -> handled.add("stale"));
      for (int i = 0; i < 5; i++) {
        int copy = i;
        dispatcher.submit(data("A", 0, true, 101, 201), 201 + i, () -> handled.add("final-" + copy));
      }
      dispatcher.submit(data("A", 0, false, 102, 202), 210, () -> handled.add("late-forming"));
      release.countDown();
      assertThat(dispatcher.shutdown(Duration.ofSeconds(2))).isTrue();
      assertThat(handled).containsExactly("final-0", "final-1", "final-2", "final-3", "final-4", "new-100");
      var snapshot = dispatcher.snapshot();
      assertThat(snapshot.receivedFinal()).isEqualTo(5);
      assertThat(snapshot.processedFinal()).isEqualTo(5);
      assertThat(snapshot.staleForming()).isEqualTo(1);
      assertThat(snapshot.failures()).isZero();
      assertThat(dispatcher.isIdle()).isTrue();
    } finally {
      release.countDown();
      dispatcher.close();
    }
  }

  @Test
  void fullFinalQueueBackpressuresWithoutLossAndShutdownDrainsAdmittedInterruptedProducer() throws Exception {
    var dispatcher = new KlineMessageDispatcher(config(1, 2));
    CountDownLatch running = new CountDownLatch(1), release = new CountDownLatch(1);
    List<String> handled = new CopyOnWriteArrayList<>();
    try (var producers = Executors.newFixedThreadPool(2)) {
      try {
        dispatcher.submit(data("BLOCK", 0, false, 1, 1), 1, () -> { running.countDown(); await(release); });
        await(running);
        dispatcher.submit(data("A", 0, true, 2, 2), 2, () -> handled.add("A"));
        var blocked = producers.submit(() -> {
          Thread.currentThread().interrupt();
          dispatcher.submit(data("B", 0, true, 3, 3), 3, () -> handled.add("B"));
          return Thread.interrupted();
        });
        assertThat(eventually(() -> dispatcher.snapshot().receivedFinal() == 2
            && dispatcher.snapshot().backpressureCount() > 0)).isTrue();
        assertThat(blocked.isDone()).isFalse();
        var stopped = producers.submit(() -> dispatcher.shutdown(Duration.ofSeconds(3)));
        release.countDown();
        assertThat(blocked.get(2, TimeUnit.SECONDS)).isTrue();
        assertThat(stopped.get(4, TimeUnit.SECONDS)).isTrue();
        assertThat(handled).containsExactly("A", "B");
        assertThat(dispatcher.snapshot().processedFinal()).isEqualTo(2);
        assertThrows(RejectedExecutionException.class,
            () -> dispatcher.submit(data("C", 0, true, 1, 1), 4, () -> { }));
      } finally {
        release.countDown();
        dispatcher.close();
      }
    }
  }

  @Test
  void fullFormingSlotsBackpressureNewKeysButPermitReplacingAnExistingKey() throws Exception {
    var dispatcher = new KlineMessageDispatcher(config(2, 1));
    CountDownLatch running = new CountDownLatch(1), release = new CountDownLatch(1);
    List<String> handled = new CopyOnWriteArrayList<>();
    try (var producer = Executors.newSingleThreadExecutor()) {
      try {
        dispatcher.submit(data("BLOCK", 0, false, 1, 1), 1, () -> { running.countDown(); await(release); });
        await(running);
        dispatcher.submit(data("A", 0, false, 1, 1), 2, () -> handled.add("old-A"));
        dispatcher.submit(data("A", 0, false, 2, 2), 3, () -> handled.add("A"));
        var blocked = producer.submit(() -> dispatcher.submit(data("B", 0, false, 1, 1), 4, () -> handled.add("B")));
        assertThat(eventually(() -> dispatcher.snapshot().backpressureCount() > 0)).isTrue();
        assertThat(blocked.isDone()).isFalse();
        release.countDown();
        blocked.get(2, TimeUnit.SECONDS);
        assertThat(dispatcher.shutdown(Duration.ofSeconds(2))).isTrue();
        assertThat(handled).containsExactly("A", "B");
      } finally {
        release.countDown();
        dispatcher.close();
      }
    }
  }

  @Test
  void disablingCoalescingStillProcessesEveryMessageWithBoundedQueues() {
    var config = config(2, 2);
    config.setCoalesceFormingUpdates(false);
    AtomicInteger forming = new AtomicInteger(), finals = new AtomicInteger();
    var dispatcher = new KlineMessageDispatcher(config);
    for (int i = 0; i < 200; i++) {
      dispatcher.submit(data("A", 0, false, i, i), i, forming::incrementAndGet);
    }
    for (int i = 0; i < 20; i++) {
      dispatcher.submit(data("A", 0, true, 200, 200), 200 + i, finals::incrementAndGet);
    }
    assertThat(dispatcher.shutdown(Duration.ofSeconds(2))).isTrue();
    assertThat(forming.get()).isEqualTo(200);
    assertThat(finals.get()).isEqualTo(20);
    assertThat(dispatcher.snapshot().coalescedForming()).isZero();
  }

  @Test
  void handlerFailureIsReportedAndDoesNotKillTheWorkerOrSkipTheNextFinal() {
    var dispatcher = new KlineMessageDispatcher(config(2, 2));
    AtomicInteger completed = new AtomicInteger();
    dispatcher.submit(data("A", 0, true, 1, 1), 1, () -> { throw new IllegalStateException("test failure"); });
    dispatcher.submit(data("A", 0, true, 2, 2), 2, completed::incrementAndGet);
    assertThat(dispatcher.shutdown(Duration.ofSeconds(2))).isTrue();
    assertThat(completed.get()).isEqualTo(1);
    assertThat(dispatcher.snapshot().failures()).isEqualTo(1);
    assertThat(dispatcher.snapshot().processedFinal()).isEqualTo(2); // attempts, failures are separate
  }
}
