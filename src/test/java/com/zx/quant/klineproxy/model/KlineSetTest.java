package com.zx.quant.klineproxy.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class KlineSetTest {
  private static Kline.StringKline bar(int trades, String close) {
    Kline.StringKline value = new Kline.StringKline();
    value.setOpenTime(0L);
    value.setCloseTime(3_599_999L);
    value.setTradeNum(trades);
    value.setClosePrice(close);
    return value;
  }

  private static KlineSet series() {
    return new KlineSet(new KlineSetKey("BTCUSDT", "1h"));
  }

  @Test
  void finalReplacesEqualTradeCountFormingAndLateFormingCannotReopenIt() {
    KlineSet set = series();
    set.commit(bar(10, "101"), false, KlineUpdateSource.STREAM, 10L, 1L);
    KlineSet.Commit result = set.commit(bar(10, "109"), true, KlineUpdateSource.STREAM, 11L, 2L);
    assertThat(result.becameFinal()).isTrue();
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(10, "109"));
    set.commit(bar(999, "999"), false, KlineUpdateSource.STREAM, 12L, 3L);
    set.commit(bar(999, "999"), false, KlineUpdateSource.REST, null, 0L);
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(10, "109"));
    assertThat(set.isFinal(0L)).isTrue();
  }

  @Test
  void sameTradeCountUsesEventTimeThenReceiveSequenceAndFinalCorrectionsAreReported() {
    KlineSet set = series();
    set.commit(bar(10, "102"), false, KlineUpdateSource.STREAM, 20L, 2L);
    set.commit(bar(10, "100"), false, KlineUpdateSource.STREAM, 19L, 3L);
    set.commit(bar(10, "101"), false, KlineUpdateSource.STREAM, 20L, 1L);
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(10, "102"));
    set.commit(bar(10, "109"), true, KlineUpdateSource.STREAM, 21L, 4L);
    set.commit(bar(10, "105"), true, KlineUpdateSource.STREAM, 20L, 5L);
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(10, "109"));
    KlineSet.Commit correction = set.commit(bar(10, "110"), true, KlineUpdateSource.STREAM, 22L, 6L);
    assertThat(correction.finalRevised()).isTrue();
    assertThat(correction.becameFinal()).isFalse();
    KlineSet.Commit duplicate = set.commit(bar(10, "110"), true, KlineUpdateSource.STREAM, 22L, 7L);
    assertThat(duplicate.updated()).isFalse();
    assertThat(duplicate.finalRevised()).isFalse();
  }

  @Test
  void absentEventTimeUsesReceiveOrderAndDoesNotBecomeAZeroTimestamp() {
    KlineSet set = series();
    set.commit(bar(10, "102"), false, KlineUpdateSource.STREAM, null, 20L);
    set.commit(bar(10, "101"), false, KlineUpdateSource.STREAM, -1L, 19L);
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(10, "102"));
    set.commit(bar(10, "103"), false, KlineUpdateSource.STREAM, -1L, 21L);
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(10, "103"));
    set.commit(bar(10, "104"), false, KlineUpdateSource.STREAM, null, 22L);
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(10, "104"));
  }

  @Test
  void restoreAndRestCannotRegressNewerLiveDataButCanFillMissingFinals() {
    KlineSet set = series();
    set.commit(bar(20, "120"), false, KlineUpdateSource.STREAM, 20L, 1L);
    set.commit(bar(10, "110"), true, KlineUpdateSource.RESTORE, null, 0L);
    assertThat(set.isFinal(0L)).isFalse();
    set.commit(bar(20, "121"), true, KlineUpdateSource.STREAM, 21L, 2L);
    set.commit(bar(20, "100"), true, KlineUpdateSource.RESTORE, null, 0L);
    set.commit(bar(20, "101"), true, KlineUpdateSource.REST, null, 0L);
    assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(20, "121"));
    KlineSet missing = series();
    assertThat(missing.commit(bar(10, "110"), true, KlineUpdateSource.RESTORE, null, 0L).becameFinal()).isTrue();
  }

  @Test
  void comparePutAndFinalPublicationAreSerializedAgainstAnInflightOlderWriter() throws Exception {
    KlineSet set = series();
    CountDownLatch olderInPut = new CountDownLatch(1);
    CountDownLatch releaseOlder = new CountDownLatch(1);
    CountDownLatch newerStarted = new CountDownLatch(1);
    ConcurrentSkipListMap<Long, Kline> controlled = new ConcurrentSkipListMap<>() {
      @Override
      public Kline put(Long key, Kline value) {
        if (value.getTradeNum() == 10) {
          olderInPut.countDown();
          try {
            if (!releaseOlder.await(5, TimeUnit.SECONDS)) {
              throw new AssertionError("older writer was not released");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
        }
        return super.put(key, value);
      }
    };
    set.setKlineMap(controlled);
    try (var pool = Executors.newFixedThreadPool(2)) {
      var older = pool.submit(() -> set.commit(bar(10, "101"), false, KlineUpdateSource.STREAM, 10L, 1L));
      try {
        assertThat(olderInPut.await(2, TimeUnit.SECONDS)).isTrue();
        var newer = pool.submit(() -> {
          newerStarted.countDown();
          return set.commit(bar(20, "109"), true, KlineUpdateSource.STREAM, 20L, 2L);
        });
        assertThat(newerStarted.await(2, TimeUnit.SECONDS)).isTrue();
        assertThrows(TimeoutException.class, () -> newer.get(30, TimeUnit.MILLISECONDS));
        releaseOlder.countDown();
        older.get(2, TimeUnit.SECONDS);
        assertThat(newer.get(2, TimeUnit.SECONDS).becameFinal()).isTrue();
        assertThat(set.getKlineMap().get(0L)).isEqualTo(bar(20, "109"));
        assertThat(set.isFinal(0L)).isTrue();
      } finally {
        releaseOlder.countDown();
      }
    }
  }
}
