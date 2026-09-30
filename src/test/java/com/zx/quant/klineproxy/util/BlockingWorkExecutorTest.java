package com.zx.quant.klineproxy.util;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BlockingWorkExecutorTest {
  @Test
  void blockedColdLoadersDoNotHoldVirtualCarriersOrDeadlockNestedCalls() throws Exception {
    try (BlockingWorkExecutor loader = new BlockingWorkExecutor("test-blocking", 2, 2);
         var requests = Executors.newVirtualThreadPerTaskExecutor()) {
      CountDownLatch entered = new CountDownLatch(2);
      CountDownLatch release = new CountDownLatch(1);
      com.github.benmanes.caffeine.cache.LoadingCache<String, Boolean> cache =
          com.github.benmanes.caffeine.cache.Caffeine.newBuilder()
              .build(key -> block(entered, release));
      try {
        var first = requests.submit(() -> loader.get(cache, "one"));
        var second = requests.submit(() -> loader.get(cache, "two"));
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        // Admission is full; this virtual thread must park, not block another carrier.
        var third = requests.submit(() -> loader.call(() -> loader.call(() -> "nested")));
        assertEquals("ready", requests.submit(() -> "ready").get(1, TimeUnit.SECONDS));
        release.countDown();
        assertFalse(first.get(3, TimeUnit.SECONDS));
        assertFalse(second.get(3, TimeUnit.SECONDS));
        assertEquals("nested", third.get(3, TimeUnit.SECONDS));
        assertFalse(requests.submit(() -> loader.get(cache, "one")).get(1, TimeUnit.SECONDS));
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  void preservesOriginalRuntimeExceptionForHttpMapping() throws Exception {
    try (BlockingWorkExecutor loader = new BlockingWorkExecutor("test-error", 1, 1);
         var requests = Executors.newVirtualThreadPerTaskExecutor()) {
      IllegalArgumentException failure = new IllegalArgumentException("bad request");
      var task = requests.submit(() -> {
        assertSame(failure, assertThrows(IllegalArgumentException.class,
            () -> loader.call(() -> { throw failure; })));
      });
      task.get(2, TimeUnit.SECONDS);
      assertEquals("recovered", requests.submit(() -> loader.call(() -> "recovered")).get(2, TimeUnit.SECONDS));
    }
  }

  private static boolean block(CountDownLatch entered, CountDownLatch release) {
    boolean virtual = Thread.currentThread().isVirtual();
    synchronized (new Object()) {
      entered.countDown();
      try {
        if (!release.await(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("test release timed out");
        }
      } catch (InterruptedException error) {
        throw new IllegalStateException(error);
      }
    }
    return virtual;
  }
}
