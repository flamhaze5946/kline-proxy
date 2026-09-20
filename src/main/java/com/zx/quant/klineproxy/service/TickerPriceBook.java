package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker.BigDecimalTicker;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Latest price per symbol, fed by an all-market ticker stream and by timestamped REST responses.
 *
 * <p>Every price carries the Binance time it was last updated, and an older write never replaces a
 * newer one. REST tickers without a time (spot ticker/price) are never merged: their freshness is
 * unknown. The symbol universe comes from full REST snapshots and dated symbol lookups; stream updates
 * for other symbols wait aside until admitted, because the futures market stream also carries
 * coin-margined contracts. Writes are serialized on the book's monitor; reads never block.
 *
 * <p>Freshness is judged per stream, not per symbol: the {@code @arr} streams only carry symbols that
 * changed, so a quiet symbol's time can be minutes old while its price is still current. The book
 * covers the market when a full snapshot taken after the start of the current continuous stream
 * segment has been applied: REST covers everything up to the snapshot, the stream every change after
 * the segment start.
 */
public final class TickerPriceBook {

  private final ConcurrentHashMap<String, BigDecimalTicker> tickers = new ConcurrentHashMap<>();

  /** latest stream update of each symbol not in the book yet */
  private final Map<String, BigDecimalTicker> pendingStream = new HashMap<>();

  /**
   * symbol -> coverage of a REST answer without a price for it: data up to that time cannot bring it
   * back. Dropped once a newer full snapshot takes over the decision.
   */
  private final Map<String, Long> absentUntil = new HashMap<>();

  private final AtomicLong version = new AtomicLong();

  private volatile Snapshot snapshot = new Snapshot(-1L, List.of());

  private final long gapMillis;

  private final long restLagMillis;

  /** latest event time seen on the stream */
  private final AtomicLong lastStreamEventTime = new AtomicLong();

  /** first event time of the current continuous stream segment */
  private final AtomicLong streamSegmentStart = new AtomicLong();

  /** time every listed symbol is known up to, from the latest full snapshot; 0 before the first one */
  private final AtomicLong lastFullSyncTime = new AtomicLong();

  /**
   * @param gapMillis a jump between consecutive frames' event times larger than this means frames were lost
   * @param restLagMillis how far a REST snapshot may trail the moment it was requested
   */
  public TickerPriceBook(long gapMillis, long restLagMillis) {
    this.gapMillis = gapMillis;
    this.restLagMillis = restLagMillis;
  }

  /**
   * Record one stream frame before applying its prices. Frames must arrive in receive order.
   * @return true when the frame starts a new continuous segment: the first frame, or one after lost frames
   */
  public boolean onStreamFrame(long minEventTime, long maxEventTime) {
    long previous = lastStreamEventTime.getAndAccumulate(maxEventTime, Math::max);
    if (previous != 0L && minEventTime - previous <= gapMillis) {
      return false;
    }
    streamSegmentStart.accumulateAndGet(minEventTime, Math::max);
    return true;
  }

  /**
   * Stream update, applied only when newer. A symbol no snapshot has listed yet keeps its latest update
   * aside until a snapshot or a dated symbol lookup admits it, so frames that arrive before the market
   * is known are not lost. One entry per symbol; a snapshot that covers its time and does not list the
   * symbol drops it (coin-margined contracts keep one live entry each).
   */
  public synchronized boolean updateFromStream(String symbol, BigDecimal price, long time) {
    if (symbol == null || price == null) {
      return false;
    }
    if (!tickers.containsKey(symbol)) {
      if (!isAbsentAt(symbol, time)) {
        pendingStream.merge(symbol, newTicker(symbol, price, time),
            (current, next) -> next.getTime() > current.getTime() ? next : current);
      }
      return false;
    }
    return upsert(symbol, price, time);
  }

  /**
   * Full-market snapshot whose tickers carry their last update time. The newest snapshot defines the
   * market: symbols it lacks are removed unless the book holds an update newer than the snapshot. An
   * older snapshot finishing late changes no membership decision: each of its prices is admitted like
   * a symbol lookup.
   * @param listed every symbol the snapshot returned, including any it could not price
   * @param requestTime server time just before the request was sent
   */
  public synchronized void applySnapshot(Collection<? extends Ticker<?>> restTickers, Set<String> listed,
      long requestTime) {
    long coverage = requestTime - restLagMillis;
    boolean newest = coverage > lastFullSyncTime.get();
    for (Ticker<?> ticker : restTickers) {
      BigDecimal price = toPrice(ticker.getPrice());
      String symbol = ticker.getSymbol();
      if (symbol == null || price == null || ticker.getTime() <= 0L) {
        continue;
      }
      if (!newest) {
        admit(symbol, price, ticker.getTime());  // late: each price counts like a symbol lookup
        continue;
      }
      Long absent = absentUntil.get(symbol);
      if (absent != null && coverage <= absent && ticker.getTime() <= absent) {
        continue;  // a no-price answer newer than both this snapshot and its price decides
      }
      upsert(symbol, price, ticker.getTime());
    }
    if (!newest || listed.isEmpty()) {
      return;
    }
    for (String symbol : List.copyOf(tickers.keySet())) {
      if (!listed.contains(symbol) && tickers.get(symbol).getTime() <= coverage) {
        tickers.remove(symbol);
        version.incrementAndGet();
      }
    }
    // an unlisted symbol's update newer than the snapshot may be a listing it could not see yet
    pendingStream.values().removeIf(pending -> {
      if (listed.contains(pending.getSymbol()) && tickers.containsKey(pending.getSymbol())) {
        upsert(pending.getSymbol(), pending.getPrice(), pending.getTime());
        return true;
      }
      return pending.getTime() <= coverage;
    });
    // older no-price answers: absent symbols now need data newer than this snapshot anyway
    absentUntil.values().removeIf(absent -> absent <= coverage);
    lastFullSyncTime.set(coverage);
  }

  /**
   * Per-symbol REST response: dated tickers are admitted, undated ones ignored.
   */
  public synchronized void applySymbols(Collection<? extends Ticker<?>> restTickers) {
    for (Ticker<?> ticker : restTickers) {
      BigDecimal price = toPrice(ticker.getPrice());
      String symbol = ticker.getSymbol();
      if (symbol == null || price == null || ticker.getTime() <= 0L) {
        continue;
      }
      admit(symbol, price, ticker.getTime());
    }
  }

  /**
   * A symbol outside the book joins only with a price newer than the latest full snapshot, which did
   * not list it; nothing a no-price answer covers gets in. Its pending stream update follows it.
   */
  private void admit(String symbol, BigDecimal price, long time) {
    if (!tickers.containsKey(symbol) && time <= lastFullSyncTime.get() || isAbsentAt(symbol, time)) {
      return;
    }
    upsert(symbol, price, time);
    BigDecimalTicker pending = pendingStream.remove(symbol);
    if (pending != null) {
      upsert(symbol, pending.getPrice(), pending.getTime());
    }
  }

  /**
   * A successful REST answer without a price for these symbols (a settling contract answers {}): each
   * leaves the book unless the book holds an update newer than the request. An answer older than the
   * latest full snapshot decides nothing: that snapshot listed the market later. Same-moment answers
   * still count: they are the more specific question.
   * @param requestTime server time just before the request was sent
   */
  public synchronized void applyAbsent(Collection<String> symbols, long requestTime) {
    long coverage = requestTime - restLagMillis;
    if (coverage < lastFullSyncTime.get()) {
      return;  // a newer full snapshot already decided which symbols the market has
    }
    for (String symbol : symbols) {
      absentUntil.merge(symbol, coverage, Math::max);
      BigDecimalTicker current = tickers.get(symbol);
      if (current != null && current.getTime() <= coverage) {
        tickers.remove(symbol);
        version.incrementAndGet();
      }
      BigDecimalTicker pending = pendingStream.get(symbol);
      if (pending != null && pending.getTime() <= coverage) {
        pendingStream.remove(symbol);
      }
    }
  }

  /** latest event time seen on the stream; 0 before the first frame */
  public long lastStreamEventTime() {
    return lastStreamEventTime.get();
  }

  public boolean hasFullSnapshot() {
    return lastFullSyncTime.get() != 0L;
  }

  /** @return true when the stream delivered an event within {@code staleAfterMillis} */
  public boolean isStreamAlive(long serverNow, long staleAfterMillis) {
    long lastEventTime = lastStreamEventTime.get();
    return lastEventTime != 0L && serverNow - lastEventTime <= staleAfterMillis;
  }

  /** @return true when a full snapshot covers the start of the current stream segment */
  public boolean isCovered() {
    return hasFullSnapshot() && lastFullSyncTime.get() >= streamSegmentStart.get();
  }

  public boolean needsFullSync() {
    return !isCovered();
  }

  /** @return every symbol ordered by name; the same list instance until the book changes */
  public List<Ticker<?>> all() {
    long currentVersion = version.get();
    Snapshot current = snapshot;
    if (current.version() == currentVersion) {
      return current.tickers();
    }
    List<Ticker<?>> values = new ArrayList<>(tickers.values());
    values.sort(Comparator.comparing(Ticker::getSymbol));
    List<Ticker<?>> immutableValues = List.copyOf(values);
    snapshot = new Snapshot(currentVersion, immutableValues);
    return immutableValues;
  }

  /** @return the known symbols among {@code symbols}, in request order */
  public List<Ticker<?>> get(Collection<String> symbols) {
    List<Ticker<?>> result = new ArrayList<>(symbols.size());
    for (String symbol : symbols) {
      BigDecimalTicker ticker = symbol == null ? null : tickers.get(symbol);
      if (ticker != null) {
        result.add(ticker);
      }
    }
    return result;
  }

  public Ticker<?> get(String symbol) {
    return symbol == null ? null : tickers.get(symbol);
  }

  public boolean contains(String symbol) {
    return symbol != null && tickers.containsKey(symbol);
  }

  /** callers hold the monitor: every write to the book is serialized, reads are not */
  private boolean upsert(String symbol, BigDecimal price, long time) {
    BigDecimalTicker current = tickers.get(symbol);
    if (current != null && current.getTime() >= time) {
      return false;
    }
    tickers.put(symbol, newTicker(symbol, price, time));
    version.incrementAndGet();
    return true;
  }

  private boolean isAbsentAt(String symbol, long time) {
    Long absent = absentUntil.get(symbol);
    return absent != null && time <= absent;
  }

  private static BigDecimalTicker newTicker(String symbol, BigDecimal price, long time) {
    BigDecimalTicker ticker = new BigDecimalTicker();
    ticker.setSymbol(symbol);
    ticker.setPrice(price);
    ticker.setTime(time);
    return ticker;
  }

  private static BigDecimal toPrice(Object price) {
    if (price == null) {
      return null;
    }
    if (price instanceof BigDecimal decimal) {
      return decimal;
    }
    return new BigDecimal(price.toString());
  }

  private record Snapshot(long version, List<Ticker<?>> tickers) {
  }
}
