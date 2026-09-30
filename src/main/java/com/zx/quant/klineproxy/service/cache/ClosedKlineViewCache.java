package com.zx.quant.klineproxy.service.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.zx.quant.klineproxy.model.EncodedKlineRows;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.util.ConvertUtil;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Share immutable closed views across arbitrary bulk combinations. Versions and time gates,
 * rather than a TTL, determine freshness. Published Klines are replaced by commit, not mutated.
 */
public final class ClosedKlineViewCache {
  private static final ObjectWriter ROW_WRITER = new ObjectMapper().writerFor(Object[].class);
  private final Cache<Kline, Row> rows = Caffeine.newBuilder()
      .weakKeys() // Identity keys: a correction is a new committed object, even at the same open time.
      .maximumWeight(16 * 1024 * 1024)
      .weigher((Kline key, Row row) -> row.weight())
      .build();
  private final Cache<Key, Window> windows = Caffeine.newBuilder()
      .maximumWeight(16 * 1024 * 1024)
      .weigher((Key key, Window window) -> window.view().rows().estimatedWeight() + 128)
      .expireAfterAccess(Duration.ofMinutes(15))
      .build();

  public View snapshot(KlineSet set, int limit, long now) {
    Key key = new Key(set.getKey(), limit);
    Window cached = windows.getIfPresent(key);
    if (cached != null && cached.owner().get() == set
        && cached.view().generation() == set.getWindowGeneration()
        && now >= cached.view().validFrom() && now < cached.view().validUntil()) {
      return cached.view();
    }
    List<Kline> selected = new ArrayList<>(Math.min(limit, 32));
    long generation;
    long validFrom = Long.MIN_VALUE;
    long validUntil = Long.MAX_VALUE;
    boolean allFinal = true;
    synchronized (set) {
      generation = set.getWindowGeneration();
      for (Kline kline : set.getKlineMap().descendingMap().values()) {
        if (kline.getCloseTime() > now) {
          validUntil = Math.min(validUntil, kline.getCloseTime());
          continue;
        }
        selected.add(kline);
        validFrom = Math.max(validFrom, kline.getCloseTime());
        allFinal &= set.isFinal(kline.getOpenTime());
        if (selected.size() == limit) {
          break;
        }
      }
    }
    Collections.reverse(selected);
    List<Object[]> display = new ArrayList<>(selected.size());
    List<String> encoded = new ArrayList<>(selected.size());
    for (Kline kline : selected) {
      // Never retain a non-final row, including a halted symbol or a timed-out finality wait.
      Row row = allFinal ? rows.getIfPresent(kline) : null;
      if (row == null) {
        row = render(kline);
        if (allFinal) {
          rows.put(kline, row);
        }
      }
      display.add(row.display());
      encoded.add(row.json());
    }
    View view = new View(new EncodedKlineRows(display, encoded), generation, validFrom, validUntil);
    if (allFinal && generation == set.getWindowGeneration()) {
      windows.put(key, new Window(new WeakReference<>(set), view));
    }
    return view;
  }

  private Row render(Kline kline) {
    Object[] display = ConvertUtil.convertToDisplayKline(kline);
    try {
      String json = ROW_WRITER.writeValueAsString(display);
      int weight = 192 + json.length() * 2;
      for (Object value : display) {
        weight += value instanceof String string ? 24 + string.length() * 2 : 24;
      }
      return new Row(display, json, weight);
    } catch (JsonProcessingException error) {
      throw new IllegalStateException("Cannot encode a display kline", error);
    }
  }

  /** Share formatting with bulk while keeping the ordinary caller's array private. */
  public Object[] display(Kline kline, boolean confirmedFinal) {
    if (!confirmedFinal || kline == null) {
      return ConvertUtil.convertToDisplayKline(kline);
    }
    Row row = rows.getIfPresent(kline);
    if (row == null) {
      row = render(kline);
      rows.put(kline, row);
    }
    return row.display().clone();
  }

  public record View(EncodedKlineRows rows, long generation, long validFrom, long validUntil) { }
  private record Row(Object[] display, String json, int weight) { }
  private record Key(KlineSetKey set, int limit) { }
  private record Window(WeakReference<KlineSet> owner, View view) { }
}
