package com.zx.quant.klineproxy.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

class KlineServiceDefaultMethodTest {

  @Test
  void queryKlineArrayShouldNotContainNullSlotsWhenReportedSizeIsLargerThanActualCollection() {
    KlineService service = new KlineService() {
      @Override
      public List<Ticker<?>> queryTickers(Collection<String> symbols) {
        return List.of();
      }

      @Override
      public List<Ticker24Hr> queryTicker24hrs(Collection<String> symbols) {
        return List.of();
      }

      @Override
      public ImmutablePair<Collection<Kline>, Integer> queryKlines(String symbol, String interval,
          Long startTime, Long endTime, int limit, boolean makeUp) {
        return ImmutablePair.of(List.of(new Kline.StringKline(), new Kline.StringKline()), 3);
      }

      @Override
      public void updateKlines(String symbol, String interval, List<Kline> klines) {
      }
    };

    Kline[] klineArray = service.queryKlineArray("BTCUSDT", "1h", null, null, 3);

    assertEquals(2, klineArray.length);
    assertFalse(Arrays.stream(klineArray).anyMatch(item -> item == null));
  }
}
