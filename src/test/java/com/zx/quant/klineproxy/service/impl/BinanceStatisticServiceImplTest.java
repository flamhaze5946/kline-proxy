package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.model.statistic.YamaDateSymbolInfo;
import com.zx.quant.klineproxy.service.KlineService;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

class BinanceStatisticServiceImplTest {
  private static final long DAY = 86_400_000L;

  @ParameterizedTest
  @ValueSource(ints = {-3, 1, 7, 48, 300})
  void rollingMeansMatchDirectWindowSumsIncludingScaleAndRounding(int window) {
    List<BigDecimal> values = variedDecimals();
    List<BigDecimal> expected = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      int start = Math.max(0, i - window + 1);
      BigDecimal sum = BigDecimal.ZERO;
      for (int j = start; j <= i; j++) {
        sum = sum.add(values.get(j));
      }
      expected.add(sum.divide(BigDecimal.valueOf(i + 1 - start), Constants.SCALE, RoundingMode.DOWN));
    }
    List<BigDecimal> actual = ReflectionTestUtils.invokeMethod(
        BinanceStatisticServiceImpl.class, "calculateRollingMeans", values, window);
    assertEquals(expected, actual);
  }

  @Test
  void rollingMeansKeepEmptyInputAndZeroWindowBehavior() {
    assertEquals(List.of(), ReflectionTestUtils.invokeMethod(
        BinanceStatisticServiceImpl.class, "calculateRollingMeans", List.of(), 0));
    assertThrows(ArithmeticException.class, () -> ReflectionTestUtils.invokeMethod(
        BinanceStatisticServiceImpl.class, "calculateRollingMeans", List.of(BigDecimal.ONE), 0));
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 1, 7, 300})
  void quoteVolumeWindowsMatchDirectSumsWithoutChangingPriceChangeAccumulation(int window) {
    BinanceStatisticServiceImpl service = new BinanceStatisticServiceImpl();
    KlineService klinesService = mock(KlineService.class);
    ReflectionTestUtils.setField(service, "binanceFutureKlineService", klinesService);
    ReflectionTestUtils.setField(service, "yama01altCoinIndexStatisticDays", 3);
    ReflectionTestUtils.setField(service, "yamaAltCoinIndexQuoteVolumeStatisticDays", window);
    List<BigDecimal> volumes = variedDecimals();
    List<Kline> klines = new ArrayList<>();
    for (int i = 0; i < volumes.size(); i++) {
      Kline.BigDecimalKline bar = new Kline.BigDecimalKline();
      bar.setOpenTime(i * DAY);
      bar.setOpenPrice(new BigDecimal("100.12345678").add(BigDecimal.valueOf(i)));
      bar.setQuoteVolume(volumes.get(i));
      klines.add(bar);
    }
    long endTime = klines.size() * DAY;
    when(klinesService.queryKlineList("BTCUSDT", "1d", 0L, endTime, Integer.MAX_VALUE)).thenReturn(klines);
    Map<Long, Map<String, YamaDateSymbolInfo>> actual = ReflectionTestUtils.invokeMethod(
        service, "calculateDateSymbolInfos", List.of("BTCUSDT"), 0L, endTime);

    List<Float> changes = new ArrayList<>();
    for (int i = 0; i < klines.size(); i++) {
      BigDecimal sum = volumes.get(i);
      for (int j = window - 1; j > 0; j--) {
        if (i - j >= 0) {
          sum = sum.add(volumes.get(i - j));
        }
      }
      BigDecimal priceChange = i < 2 ? BigDecimal.ZERO
          : ((Kline.BigDecimalKline) klines.get(i)).getOpenPrice()
              .divide(((Kline.BigDecimalKline) klines.get(i - 2)).getOpenPrice(), 8, RoundingMode.DOWN)
              .subtract(BigDecimal.ONE);
      changes.add(priceChange.floatValue());
      float changeSum = changes.get(i);
      for (int j = window - 1; j > 0; j--) {
        if (i - j >= 0) {
          changeSum += changes.get(i - j);
        }
      }
      YamaDateSymbolInfo info = actual.get(i * DAY).get("BTCUSDT");
      assertEquals(Float.floatToIntBits(sum.floatValue()), Float.floatToIntBits(info.getQuoteVolumeSum()));
      assertEquals(Float.floatToIntBits(priceChange.floatValue()), Float.floatToIntBits(info.getPctChange()));
      assertEquals(Float.floatToIntBits(changeSum), Float.floatToIntBits(info.getPctChangeSum()));
    }
  }

  private static List<BigDecimal> variedDecimals() {
    List<BigDecimal> values = new ArrayList<>(List.of(
        new BigDecimal("100000000000000000000.123456789123456789"),
        new BigDecimal("-100000000000000000000.123456789123456789"),
        new BigDecimal("0.000000000000000001"), BigDecimal.ZERO,
        new BigDecimal("-0.999999999999999999"), new BigDecimal("1E+10")));
    Random random = new Random(5946L);
    for (int i = 0; i < 200; i++) {
      values.add(BigDecimal.valueOf(random.nextInt(2_000_001) - 1_000_000, i % 19));
    }
    return values;
  }
}
