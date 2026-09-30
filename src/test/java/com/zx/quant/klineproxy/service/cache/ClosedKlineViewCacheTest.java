package com.zx.quant.klineproxy.service.cache;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.config.SerializeConfig;
import com.zx.quant.klineproxy.model.BulkKlinesResponse;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.KlineUpdateSource;
import com.zx.quant.klineproxy.util.ConvertUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ClosedKlineViewCacheTest {
  private static final long H = 3_600_000;

  @Test
  void overlappingWindowsShareFormattingAndKeepExistingJsonContract() throws Exception {
    ClosedKlineViewCache cache = new ClosedKlineViewCache();
    KlineSet set = series();
    Kline first = bar(0, 0.000000019, 10), second = bar(H, 12345.678912345, 20);
    commit(set, first, true); commit(set, second, true);
    var two = cache.snapshot(set, 2, H * 2);
    var one = cache.snapshot(set, 1, H * 2);
    assertSame(two, cache.snapshot(set, 2, H * 2 + 10));
    assertSame(two.rows().get(1)[4], one.rows().getFirst()[4]);
    ObjectMapper mapper = new SerializeConfig().objectMapper();
    var actual = new BulkKlinesResponse("1h", H * 2, Map.of("BTCUSDT", two.rows()),
        false, List.of("BTCUSDT"), 25, List.of("HALTED"));
    var expected = new BulkKlinesResponse("1h", H * 2,
        Map.of("BTCUSDT", List.of(ConvertUtil.convertToDisplayKline(first),
            ConvertUtil.convertToDisplayKline(second))), false, List.of("BTCUSDT"), 25,
        List.of("HALTED"));
    assertEquals(mapper.writeValueAsString(expected), mapper.writeValueAsString(actual));
    assertEquals(mapper.readTree(mapper.writeValueAsBytes(expected)),
        mapper.readTree(mapper.writeValueAsBytes(actual)));
  }

  @Test
  void correctionAndSeriesReplacementCannotReuseOldEncodedBytes() throws Exception {
    ClosedKlineViewCache cache = new ClosedKlineViewCache();
    KlineSet set = series();
    commit(set, bar(0, 10, 1), true);
    var before = cache.snapshot(set, 10, H);
    commit(set, bar(0, 11, 2), true);
    var after = cache.snapshot(set, 10, H);
    assertNotSame(before.rows(), after.rows());
    assertEquals("11", after.rows().getFirst()[4]);
    KlineSet replacement = series();
    commit(replacement, bar(0, 12, 1), true);
    assertEquals("12", cache.snapshot(replacement, 10, H).rows().getFirst()[4]);
    assertEquals("10", before.rows().getFirst()[4]);
  }

  @Test
  void closeTimeGateInvalidatesBeforeBoundaryAndNonFinalUpdatesStayVisible() {
    ClosedKlineViewCache cache = new ClosedKlineViewCache();
    KlineSet set = series();
    commit(set, bar(0, 10, 1), true);
    commit(set, bar(H, 11, 1), false);
    var before = cache.snapshot(set, 10, H * 2 - 2);
    assertEquals(1, before.rows().size());
    var atCloseTime = cache.snapshot(set, 10, H * 2 - 1);
    assertEquals(2, atCloseTime.rows().size());
    commit(set, bar(H, 12, 2), false);
    assertEquals("12", cache.snapshot(set, 10, H * 2).rows().getLast()[4]);
    commit(set, bar(H, 13, 2), true);
    assertEquals("13", cache.snapshot(set, 10, H * 2).rows().getLast()[4]);
    assertEquals(1, cache.snapshot(set, 10, H * 2 - 2).rows().size(),
        "A server-clock correction must not include a bar before its time gate");
  }

  @Test
  void newlyClosedWindowReusesOlderFinalRowsAndTrimmingInvalidatesTheWindow() {
    ClosedKlineViewCache cache = new ClosedKlineViewCache();
    KlineSet set = series();
    commit(set, bar(0, 10, 1), true);
    commit(set, bar(H, 11, 1), true);
    var before = cache.snapshot(set, 10, H * 2);
    commit(set, bar(H * 2, 12, 1), true);
    var after = cache.snapshot(set, 10, H * 3);
    assertSame(before.rows().getFirst()[4], after.rows().getFirst()[4]);
    synchronized (set) {
      set.getKlineMap().pollFirstEntry();
      set.dropFinalBefore(H);
    }
    assertEquals(2, cache.snapshot(set, 10, H * 3).rows().size());
  }

  @Test
  void programmaticRowMutationCannotPoisonOtherResponsesOrCachedJson() throws Exception {
    ClosedKlineViewCache cache = new ClosedKlineViewCache();
    KlineSet set = series();
    Kline kline = bar(0, 123.456, 10);
    commit(set, kline, true);
    var view = cache.snapshot(set, 10, H);
    ObjectMapper mapper = new SerializeConfig().objectMapper();
    String wire = mapper.writeValueAsString(view.rows());
    Object[] callerRow = view.rows().getFirst();
    callerRow[4] = "corrupted";
    assertEquals("123.456", view.rows().getFirst()[4]);
    assertEquals("123.456", cache.display(kline, true)[4]);
    assertEquals(wire, mapper.writeValueAsString(view.rows()));
  }

  @Test
  void nullAndEscapedStringFieldsKeepJsonParity() throws Exception {
    Kline.StringKline kline = new Kline.StringKline();
    kline.setOpenTime(0); kline.setCloseTime(H - 1);
    kline.setClosePrice("quote\" newline\n slash\\ unicode币");
    KlineSet set = series(); commit(set, kline, true);
    var actual = new ClosedKlineViewCache().snapshot(set, 10, H).rows();
    List<Object[]> expected = new ArrayList<>(); expected.add(ConvertUtil.convertToDisplayKline(kline));
    ObjectMapper mapper = new SerializeConfig().objectMapper();
    assertEquals(mapper.writeValueAsString(expected), mapper.writeValueAsString(actual));
  }

  private static KlineSet series() {
    return new KlineSet(new KlineSetKey("BTCUSDT", "1h"));
  }

  private static void commit(KlineSet set, Kline kline, boolean closed) {
    set.commit(kline, closed, KlineUpdateSource.STREAM, kline.getTradeNum() * 100L,
        kline.getTradeNum());
  }

  private static Kline bar(long open, double close, int trades) {
    Kline.DoubleKline kline = new Kline.DoubleKline();
    kline.setOpenTime(open); kline.setCloseTime(open + H - 1);
    kline.setOpenPrice(close); kline.setHighPrice(close); kline.setLowPrice(close);
    kline.setClosePrice(close); kline.setVolume(0.000000019); kline.setQuoteVolume(123.456789123);
    kline.setTradeNum(trades);
    return kline;
  }
}
