package com.zx.quant.klineproxy.model;

import java.util.Set;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * kline set key
 * @author flamhaze5946
 */
@Data
public class KlineSet {

  private final KlineSetKey key;

  private ConcurrentSkipListMap<Long, Kline> klineMap = new ConcurrentSkipListMap<>();

  /**
   * Open times whose kline is FINAL: the websocket sent the closing update (x=true), the bar came
   * from the REST API as a closed candle, it was restored from disk, or it is a synthetic fill.
   * Bulk queries with closed_only=true block on the just-closed bar until it is in this set.
   */
  private final Set<Long> finalOpenTimes = ConcurrentHashMap.newKeySet();

  // Only bars actually observed from the stream carry revision metadata. Trim with retained bars,
  // so a late duplicate cannot regress an older bar still served from this cache.
  @Getter(AccessLevel.NONE)
  @ToString.Exclude
  @EqualsAndHashCode.Exclude
  private final NavigableMap<Long, StreamVersion> streamVersions = new TreeMap<>();

  /**
   * One commit lock covers compare, replacement and final publication for ALL writers, including
   * REST and restore. A forming snapshot can never replace a final bar. The map's own concurrent
   * get/put methods alone do not make a conditional update atomic.
   */
  public synchronized Commit commit(Kline incoming, boolean closed, KlineUpdateSource source,
      Long eventTime, long receiveSequence) {
    long openTime = incoming.getOpenTime();
    Kline existing = klineMap.get(openTime);
    boolean wasFinal = finalOpenTimes.contains(openTime);
    StreamVersion previousVersion = streamVersions.get(openTime);
    if (existing != null) {
      if (!closed && wasFinal) {
        return Commit.UNCHANGED;
      }
      // The first final is authoritative even when n is unchanged. Once final, reject older n.
      if ((!closed || wasFinal || source != KlineUpdateSource.STREAM)
          && incoming.getTradeNum() < existing.getTradeNum()) {
        return Commit.UNCHANGED;
      }
      if (incoming.getTradeNum() == existing.getTradeNum() && (!closed || wasFinal)) {
        if (source == KlineUpdateSource.SYNTHETIC || source == KlineUpdateSource.RESTORE) {
          return Commit.UNCHANGED;
        }
        if (previousVersion != null) {
          if (source != KlineUpdateSource.STREAM || previousVersion.newerThan(eventTime, receiveSequence)) {
            return Commit.UNCHANGED;
          }
        }
      }
    }
    boolean changed = existing == null || !existing.equals(incoming);
    if (changed) {
      klineMap.put(openTime, incoming);
    }
    if (source == KlineUpdateSource.STREAM) {
      streamVersions.put(openTime, new StreamVersion(eventTime == null ? 0L : eventTime,
          receiveSequence, eventTime != null));
    } else if (changed) {
      streamVersions.remove(openTime);
    }
    boolean becameFinal = closed && finalOpenTimes.add(openTime);
    return new Commit(changed, becameFinal, changed && wasFinal);
  }

  public record Commit(boolean updated, boolean becameFinal, boolean finalRevised) {
    public static final Commit UNCHANGED = new Commit(false, false, false);
  }

  // Keep the timestamp primitive: an extra boxed Long per retained streaming bar is substantial
  // across thousands of series. The presence bit preserves the distinction between zero and absent.
  private record StreamVersion(long eventTime, long receiveSequence, boolean hasEventTime) {
    private boolean newerThan(Long candidateEventTime, long candidateSequence) {
      if (hasEventTime && candidateEventTime != null && eventTime != candidateEventTime) {
        return eventTime > candidateEventTime;
      }
      return receiveSequence > 0 && candidateSequence > 0 && receiveSequence > candidateSequence;
    }
  }

  public boolean isFinal(long openTime) {
    return finalOpenTimes.contains(openTime);
  }

  /** Copy references under the commit lock; expensive persistence encoding happens afterwards. */
  public synchronized List<Kline> finalSnapshot(long now) {
    return klineMap.values().stream()
        .filter(kline -> kline.getCloseTime() < now && finalOpenTimes.contains(kline.getOpenTime()))
        .toList();
  }

  /** @return true when the flag was newly set */
  public synchronized boolean markFinal(long openTime) {
    return finalOpenTimes.add(openTime);
  }

  public synchronized void dropFinalBefore(long firstOpenTime) {
    finalOpenTimes.removeIf(openTime -> openTime < firstOpenTime);
    streamVersions.headMap(firstOpenTime, false).clear();
  }

  public KlineSet(KlineSetKey key) {
    this.key = key;
  }
}
