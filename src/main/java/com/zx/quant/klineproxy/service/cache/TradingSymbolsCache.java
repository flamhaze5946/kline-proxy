package com.zx.quant.klineproxy.service.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** A projection of one exchange-info publication; server-time changes do not change symbols. */
public final class TradingSymbolsCache<T> {
  private final Function<T, List<String>> select;
  private volatile Snapshot<T> current;

  public TradingSymbolsCache(Function<T, List<String>> select) {
    this.select = select;
  }

  public List<String> symbols(T exchange) {
    return snapshot(exchange).symbols();
  }

  public Set<String> symbolSet(T exchange) {
    return snapshot(exchange).symbolSet();
  }

  private Snapshot<T> snapshot(T exchange) {
    Snapshot<T> snapshot = current;
    if (snapshot != null && snapshot.source() == exchange) {
      return snapshot;
    }
    // Exchange retrieval happens before entering this CPU-only publication lock.
    synchronized (this) {
      snapshot = current;
      if (snapshot == null || snapshot.source() != exchange) {
        List<String> symbols = Collections.unmodifiableList(new ArrayList<>(select.apply(exchange)));
        snapshot = new Snapshot<>(exchange, symbols,
            Collections.unmodifiableSet(new HashSet<>(symbols)));
        current = snapshot;
      }
      return snapshot;
    }
  }

  private record Snapshot<T>(T source, List<String> symbols, Set<String> symbolSet) { }
}
