package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.Executor;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class AbstractKlineServiceTaskExecutionTest {

  @Test
  void shouldLimitConcurrentTaskExecutionToConfiguredWorkerCount() {
    ExecutorService executor = Executors.newCachedThreadPool();
    AtomicInteger currentRunning = new AtomicInteger(0);
    AtomicInteger maxRunning = new AtomicInteger(0);
    List<Runnable> tasks = IntStream.range(0, 12)
        .<Runnable>mapToObj(ignore -> () -> {
          int running = currentRunning.incrementAndGet();
          maxRunning.accumulateAndGet(running, Math::max);
          try {
            Thread.sleep(30L);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          } finally {
            currentRunning.decrementAndGet();
          }
        })
        .toList();

    try {
      AbstractKlineService.runTasksWithLimitedWorkers(tasks, 3, executor);
    } finally {
      executor.shutdownNow();
    }

    assertEquals(0, currentRunning.get());
    assertTrue(maxRunning.get() <= 3);
  }

  @Test
  void shouldRunInlineWhenWorkerCountIsOne() {
    AtomicInteger executed = new AtomicInteger(0);
    List<Runnable> tasks = IntStream.range(0, 3)
        .<Runnable>mapToObj(ignore -> executed::incrementAndGet)
        .toList();
    Executor failingExecutor = command -> fail("executor should not be used when maxWorkers is one");

    AbstractKlineService.runTasksWithLimitedWorkers(tasks, 1, failingExecutor);

    assertEquals(3, executed.get());
  }
}
