package com.zx.quant.klineproxy.service.cache;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class PublicationCacheTest {
  @Test
  void neverHashesMutableMetadataAndRefreshesOnIdentityChange() {
    class Metadata {
      int serverTime;
      @Override public int hashCode() { throw new AssertionError("Expensive metadata hash must not run"); }
      @Override public boolean equals(Object value) { throw new AssertionError("Use identity"); }
    }
    var cache = new PublicationCache<Metadata, Object>();
    var first = new Metadata();
    var builds = new AtomicInteger();
    java.util.function.Function<Metadata, Object> project = metadata -> {
      builds.incrementAndGet(); return new Object();
    };
    Object value = cache.get(first, project);
    first.serverTime++;
    for (int i = 0; i < 10_000; i++) assertSame(value, cache.get(first, project));
    assertEquals(1, builds.get());
    assertNotSame(value, cache.get(new Metadata(), project));
    assertEquals(2, builds.get());
  }
}
