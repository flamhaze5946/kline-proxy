package com.zx.quant.klineproxy.util;

import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Keeps Java 21 monitor-blocking I/O off virtual-thread carriers, with bounded admission. */
public final class BlockingWorkExecutor implements AutoCloseable {
  private final Semaphore admission;
  private final ThreadPoolExecutor executor;

  public BlockingWorkExecutor(String name, int workers, int capacity) {
    if (workers < 1 || capacity < workers) {
      throw new IllegalArgumentException("capacity must cover positive worker count");
    }
    admission = new Semaphore(capacity);
    AtomicInteger sequence = new AtomicInteger();
    executor = new ThreadPoolExecutor(workers, workers, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(capacity), task -> {
          Thread thread = new Thread(task, name + "-" + sequence.incrementAndGet());
          thread.setDaemon(true);
          return thread;
        });
    executor.allowCoreThreadTimeOut(true);
  }

  public <T> T call(Supplier<T> work) {
    if (!Thread.currentThread().isVirtual()) {
      // Includes nested calls on this pool: no worker may enqueue and wait for its own pool.
      return work.get();
    }
    admission.acquireUninterruptibly(); // Parks outside any cache-loader monitor.
    CompletableFuture<T> result = new CompletableFuture<>();
    try {
      executor.execute(() -> {
        try {
          result.complete(work.get());
        } catch (Throwable error) {
          result.completeExceptionally(error);
        } finally {
          admission.release();
        }
      });
    } catch (RuntimeException error) {
      admission.release();
      throw error;
    }
    try {
      return result.join();
    } catch (CompletionException error) {
      if (error.getCause() instanceof RuntimeException cause) {
        throw cause;
      }
      if (error.getCause() instanceof Error cause) {
        throw cause;
      }
      throw error;
    }
  }

  /** Cache hits need no handoff; the entire synchronous loader runs outside virtual carriers. */
  public <K, V> V get(LoadingCache<K, V> cache, K key) {
    V cached = cache.getIfPresent(key);
    return cached != null ? cached : call(() -> cache.get(key));
  }

  @Override
  public void close() {
    executor.shutdown();
  }
}
