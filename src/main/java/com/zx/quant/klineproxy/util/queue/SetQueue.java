package com.zx.quant.klineproxy.util.queue;

import java.util.Collection;

/**
 * set queue, exists item will not be added again before removed
 * @author flamhaze5946
 */
public interface SetQueue<E> {

  boolean isEmpty();

  int size();

  void clear();

  boolean offer(E item);

  default void offerAll(Collection<E> items) {
    synchronized (this) {
      for (E item : items) {
        offer(item);
      }
    }
  }

  E poll();
}
