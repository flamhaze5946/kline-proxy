package com.zx.quant.klineproxy.service;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Per-service subscriptions: a close only wakes requests waiting for that exact bar. */
public final class FinalBarWaitRegistry {
  private final ConcurrentHashMap<Key, Set<Registration>> listeners = new ConcurrentHashMap<>();

  public record Key(String symbol, String interval, long openTime) { }

  /** Register first, then recheck the store: a close preceding registration is already visible. */
  public Registration register(Collection<Key> keys) {
    Registration registration = new Registration(List.copyOf(new HashSet<>(keys)));
    for (Key key : registration.keys) {
      listeners.compute(key, (ignored, current) -> {
        Set<Registration> bucket = current == null ? new HashSet<>() : current;
        bucket.add(registration);
        return bucket;
      });
    }
    return registration;
  }

  public void signal(Key key) {
    // Removal is atomic with registration/cleanup. The detached bucket is no longer mutated.
    Set<Registration> ready = listeners.remove(key);
    if (ready != null) {
      ready.forEach(Registration::signal);
    }
  }

  public void signal(String symbol, String interval, long openTime) {
    if (!listeners.isEmpty()) {
      signal(new Key(symbol, interval, openTime));
    }
  }

  public int subscribedKeyCount() {
    return listeners.size();
  }

  public final class Registration implements AutoCloseable {
    private final List<Key> keys;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile long version;

    private Registration(List<Key> keys) {
      this.keys = keys;
    }

    /** Capture BEFORE checking pending bars, so a notification during that check is retained. */
    public long version() {
      return version;
    }

    public void awaitChange(long observedVersion, long timeoutNanos) throws InterruptedException {
      lock.lockInterruptibly();
      try {
        while (version == observedVersion && !closed.get() && timeoutNanos > 0) {
          timeoutNanos = changed.awaitNanos(timeoutNanos);
        }
      } finally {
        lock.unlock();
      }
    }

    private void signal() {
      lock.lock();
      try {
        version++;
        changed.signalAll();
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      for (Key key : keys) {
        listeners.computeIfPresent(key, (ignored, bucket) -> {
          bucket.remove(this);
          return bucket.isEmpty() ? null : bucket;
        });
      }
      signal();
    }
  }
}
