package com.zx.quant.klineproxy.service.cache;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TradingSymbolsCacheTest {
  @Test
  void refreshReplacesBothViewsWithoutRepeatedFilteringOrChangingOrder() {
    AtomicInteger scans = new AtomicInteger();
    TradingSymbolsCache<List<String>> cache = new TradingSymbolsCache<>(source -> {
      scans.incrementAndGet(); return source;
    });
    List<String> first = List.of("B", "A", "B");
    assertEquals(first, cache.symbols(first));
    assertEquals(Set.of("A", "B"), cache.symbolSet(first));
    assertSame(cache.symbolSet(first), cache.symbolSet(first));
    assertEquals(1, scans.get());
    assertThrows(UnsupportedOperationException.class, () -> cache.symbols(first).add("C"));
    assertThrows(UnsupportedOperationException.class, () -> cache.symbolSet(first).add("C"));
    List<String> next = List.of("B", "C");
    assertEquals(Set.of("B", "C"), cache.symbolSet(next));
    assertEquals(next, cache.symbols(next));
    assertEquals(2, scans.get());
  }
}
