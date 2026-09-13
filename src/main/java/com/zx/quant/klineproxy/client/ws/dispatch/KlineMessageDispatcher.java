package com.zx.quant.klineproxy.client.ws.dispatch;

import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import com.zx.quant.klineproxy.model.KlineDispatchMetadata.Bar;
import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

/**
 * Single writer per series, final FIFO and bounded latest-forming slots. Valid closing tasks are
 * never evicted or coalesced. Full queues apply producer backpressure; shutdown drains submissions
 * already admitted, including producers waiting for capacity. Crash durability is outside this
 * in-memory dispatcher's contract.
 */
@Slf4j
public final class KlineMessageDispatcher implements AutoCloseable {
  private static final int FINAL_BURST = 64;
  private final Shard[] shards;
  private final int finalCapacity;
  private final int formingCapacity;
  private final int closedCapacity;
  private final boolean coalesce;
  private final LongAdder receivedFinal = new LongAdder();
  private final LongAdder processedFinal = new LongAdder();
  private final LongAdder receivedForming = new LongAdder();
  private final LongAdder processedForming = new LongAdder();
  private final LongAdder coalescedForming = new LongAdder();
  private final LongAdder staleForming = new LongAdder();
  private final LongAdder failures = new LongAdder();
  private final LongAdder backpressure = new LongAdder();
  private final LongAdder backpressureNanos = new LongAdder();
  private final LongAdder queuedFinal = new LongAdder();
  private final LongAdder queuedForming = new LongAdder();
  private final LongAdder active = new LongAdder();

  public KlineMessageDispatcher(KlineIngressProperties properties) {
    properties.validate();
    finalCapacity = properties.getFinalQueueCapacityPerWorker();
    formingCapacity = properties.getFormingQueueCapacityPerWorker();
    closedCapacity = properties.getClosedKeyCapacityPerWorker();
    coalesce = properties.isCoalesceFormingUpdates();
    shards = new Shard[properties.getWorkers()];
    for (int i = 0; i < shards.length; i++) {
      shards[i] = new Shard(i);
    }
  }

  public void submit(KlineDispatchMetadata metadata, long sequence, Runnable task) {
    shards[Math.floorMod(metadata.series().hashCode(), shards.length)]
        .submit(new Task(metadata, sequence, task));
  }

  public int queuedTasks() {
    return (int) (queuedFinal.sum() + queuedForming.sum());
  }

  public int activeWorkers() {
    return active.intValue();
  }

  public int workerCount() {
    return shards.length;
  }

  public boolean isIdle() {
    for (Shard shard : shards) {
      shard.lock.lock();
      try {
        if (shard.submitting != 0 || shard.running || !shard.empty()) {
          return false;
        }
      } finally {
        shard.lock.unlock();
      }
    }
    return true;
  }

  public Snapshot snapshot() {
    return new Snapshot(receivedFinal.sum(), processedFinal.sum(), receivedForming.sum(),
        processedForming.sum(), coalescedForming.sum(), staleForming.sum(), failures.sum(),
        backpressure.sum(), backpressureNanos.sum(), queuedFinal.sum(), queuedForming.sum(), active.sum());
  }

  public record Snapshot(long receivedFinal, long processedFinal, long receivedForming,
      long processedForming, long coalescedForming, long staleForming, long failures,
      long backpressureCount, long backpressureNanos, long queuedFinal, long queuedForming,
      long activeWorkers) { }

  public boolean shutdown(Duration timeout) {
    for (Shard shard : shards) {
      shard.lock.lock();
      try {
        shard.accepting = false;
        shard.work.signalAll();
      } finally {
        shard.lock.unlock();
      }
    }
    long deadline = System.nanoTime() + timeout.toNanos();
    for (Shard shard : shards) {
      Thread thread = shard.thread;
      if (thread != null) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
          return false;
        }
        try {
          TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
        if (thread.isAlive()) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (!shutdown(Duration.ofSeconds(30))) {
      log.error("Kline ingress shutdown did not drain accepted tasks: {}", snapshot());
    }
  }

  private record Task(KlineDispatchMetadata metadata, long sequence, Runnable action) { }

  private final class Shard implements Runnable {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition work = lock.newCondition();
    private final Condition space = lock.newCondition();
    private final ArrayDeque<Task> finals = new ArrayDeque<>();
    private final ArrayDeque<Task> formingFifo = new ArrayDeque<>();
    private final LinkedHashMap<Bar, Task> forming = new LinkedHashMap<>();
    private final LinkedHashMap<Bar, Boolean> closedKeys = new LinkedHashMap<>();
    private final int index;
    private Thread thread;
    private boolean accepting = true;
    private boolean running;
    private int submitting;
    private int finalBurst;

    private Shard(int index) {
      this.index = index;
    }

    private boolean empty() {
      return finals.isEmpty() && forming.isEmpty() && formingFifo.isEmpty();
    }

    private void submit(Task task) {
      Bar key = task.metadata.bar();
      lock.lock();
      try {
        if (!accepting) {
          throw new RejectedExecutionException("Kline ingress is shutting down");
        }
        submitting++;
        if (thread == null) {
          thread = new Thread(this, "websocket-message-handler-kline-" + index);
          thread.setDaemon(true);
          thread.start();
        }
        try {
          if (task.metadata.closed()) {
            receivedFinal.increment();
            while (finals.size() >= finalCapacity) {
              awaitCapacity();
            }
            finals.addLast(task);
            queuedFinal.increment();
            if (coalesce) {
              if (forming.remove(key) != null) {
                queuedForming.decrement();
                coalescedForming.increment();
              }
              closedKeys.put(key, Boolean.TRUE);
              if (closedKeys.size() > closedCapacity) {
                closedKeys.pollFirstEntry();
              }
            }
          } else {
            receivedForming.increment();
            while (true) {
              if (coalesce && closedKeys.containsKey(key)) {
                coalescedForming.increment();
                return;
              }
              Task previous = coalesce ? forming.get(key) : null;
              if (previous != null) {
                if (task.metadata.supersedes(previous.metadata, task.sequence, previous.sequence)) {
                  forming.put(key, task);
                  coalescedForming.increment();
                } else {
                  staleForming.increment();
                }
                return;
              }
              if ((coalesce ? forming.size() : formingFifo.size()) < formingCapacity) {
                if (coalesce) {
                  forming.put(key, task);
                } else {
                  formingFifo.addLast(task);
                }
                queuedForming.increment();
                break;
              }
              awaitCapacity();
            }
          }
          work.signal();
          space.signalAll();
        } finally {
          submitting--;
          work.signal();
        }
      } finally {
        lock.unlock();
      }
    }

    private void awaitCapacity() {
      backpressure.increment();
      long start = System.nanoTime();
      // Interruption does not turn an already admitted close into a silent drop.
      space.awaitUninterruptibly();
      backpressureNanos.add(System.nanoTime() - start);
    }

    private Task take() {
      lock.lock();
      try {
        while (empty()) {
          if (!accepting && submitting == 0) {
            return null;
          }
          work.awaitUninterruptibly();
        }
        Task task;
        boolean hasForming = !forming.isEmpty() || !formingFifo.isEmpty();
        if (!finals.isEmpty() && (finalBurst < FINAL_BURST || !hasForming)) {
          task = finals.removeFirst();
          queuedFinal.decrement();
          finalBurst++;
        } else {
          task = coalesce ? forming.pollFirstEntry().getValue() : formingFifo.removeFirst();
          queuedForming.decrement();
          finalBurst = 0;
        }
        running = true;
        active.increment();
        space.signalAll();
        return task;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void run() {
      Task task;
      while ((task = take()) != null) {
        try {
          task.action.run();
        } catch (Throwable error) {
          failures.increment();
          log.error("Kline ingress handler failed for {} closed={}", task.metadata.bar(),
              task.metadata.closed(), error);
        } finally {
          if (task.metadata.closed()) {
            processedFinal.increment();
          } else {
            processedForming.increment();
          }
          lock.lock();
          try {
            running = false;
            active.decrement();
            work.signalAll();
          } finally {
            lock.unlock();
          }
        }
      }
    }
  }
}
