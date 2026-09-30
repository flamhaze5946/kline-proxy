package com.zx.quant.klineproxy.service.cache;

import java.util.function.Function;

/** One immutable projection of one metadata publication, compared only by identity. */
public final class PublicationCache<K, V> {
  private volatile Entry<K, V> current;

  public V get(K source, Function<? super K, ? extends V> project) {
    Entry<K, V> entry = current;
    if (entry != null && entry.source() == source) {
      return entry.value();
    }
    // Only CPU-local projection belongs here; fetch the source before calling get.
    synchronized (this) {
      entry = current;
      if (entry == null || entry.source() != source) {
        entry = new Entry<>(source, project.apply(source));
        current = entry;
      }
      return entry.value();
    }
  }

  private record Entry<K, V>(K source, V value) { }
}
