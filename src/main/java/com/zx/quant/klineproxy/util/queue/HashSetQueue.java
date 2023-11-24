package com.zx.quant.klineproxy.util.queue;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * hash set queue
 * @param <E> item type
 */
public class HashSetQueue<E> implements SetQueue<E> {

  private final Queue<E> targetQueue;

  private final Set<E> set;

  public HashSetQueue() {
    this.targetQueue = new LinkedBlockingQueue<>();
    this.set = new HashSet<>();
  }

  @Override
  public boolean isEmpty() {
    return targetQueue.isEmpty();
  }

  @Override
  public int size() {
    return targetQueue.size();
  }

  @Override
  public void clear() {
    synchronized (this) {
      targetQueue.clear();
      set.clear();
    }
  }

  @Override
  public boolean offer(E item) {
    if (item == null) {
      throw new RuntimeException("item shouldn't be null.");
    }
    synchronized (this) {
      if (set.contains(item)) {
        return false;
      }
      set.add(item);
      targetQueue.offer(item);
      return true;
    }
  }

  @Override
  public E poll() {
    if (set.isEmpty()) {
      return null;
    }
    synchronized (this) {
      E item = targetQueue.poll();
      set.remove(item);
      return item;
    }
  }
}
