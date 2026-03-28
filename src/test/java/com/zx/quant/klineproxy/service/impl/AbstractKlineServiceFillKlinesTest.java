package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.StringKline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import java.util.List;
import org.junit.jupiter.api.Test;

class AbstractKlineServiceFillKlinesTest {

  @Test
  void updateKlinesShouldFillMissingKlineWithPreviousCloseAndBinanceStyleCloseTime() {
    TestKlineService service = new TestKlineService();
    StringKline first = new StringKline();
    first.setOpenTime(0);
    first.setCloseTime(IntervalEnum.ONE_HOUR.getMills() - 1);
    first.setOpenPrice("90");
    first.setHighPrice("110");
    first.setLowPrice("80");
    first.setClosePrice("100");
    first.setTradeNum(10);

    StringKline third = new StringKline();
    third.setOpenTime(IntervalEnum.ONE_HOUR.getMills() * 2);
    third.setCloseTime((IntervalEnum.ONE_HOUR.getMills() * 3) - 1);
    third.setOpenPrice("120");
    third.setHighPrice("130");
    third.setLowPrice("110");
    third.setClosePrice("125");
    third.setTradeNum(20);

    service.updateKlines("BTCUSDT", IntervalEnum.ONE_HOUR.code(), List.of(first, third));

    KlineSet klineSet = service.klineSetMap.get(new KlineSetKey("BTCUSDT", IntervalEnum.ONE_HOUR.code()));
    assertNotNull(klineSet);
    StringKline filled = (StringKline) klineSet.getKlineMap().get(IntervalEnum.ONE_HOUR.getMills());
    assertNotNull(filled);
    assertEquals("100", filled.getOpenPrice());
    assertEquals("100", filled.getHighPrice());
    assertEquals("100", filled.getLowPrice());
    assertEquals("100", filled.getClosePrice());
    assertEquals(0, filled.getTradeNum());
    assertEquals((IntervalEnum.ONE_HOUR.getMills() * 2) - 1, filled.getCloseTime());
  }

  private static class TestKlineService extends AbstractKlineService<WebSocketClient> {

    @Override
    protected List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
      return List.of();
    }

    @Override
    protected List<Ticker24Hr> queryTicker24Hrs0() {
      return List.of();
    }

    @Override
    protected String getRateLimiterName() {
      return "test";
    }

    @Override
    protected List<String> getSymbols() {
      return List.of();
    }

    @Override
    protected KlineSyncConfigProperties getSyncConfig() {
      return new KlineSyncConfigProperties();
    }

    @Override
    protected int getMakeUpKlinesLimit() {
      return 1;
    }

    @Override
    protected int getMakeUpKlinesWeight() {
      return 1;
    }

    @Override
    protected int getTicker24HrsWeight() {
      return 1;
    }

    @Override
    protected String getServiceType() {
      return "test";
    }
  }
}
