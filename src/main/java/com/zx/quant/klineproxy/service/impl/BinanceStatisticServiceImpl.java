package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.BigDecimalKline;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.service.StatisticService;
import com.zx.quant.klineproxy.util.Serializer;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * binance statistic service impl
 * @author flamhaze5946
 */
@Slf4j
@Service("binanceStatisticService")
public class BinanceStatisticServiceImpl implements StatisticService {

  private static final IntervalEnum ATR_INTERVAL = IntervalEnum.ONE_HOUR;

  @Autowired
  private Serializer serializer;

  @Qualifier("binanceSpotKlineService")
  @Autowired
  private KlineService klineService;

  @Value("${statistic.binance.atr.symbol:BTCUSDT}")
  private String atrSymbol;

  @Value("${statistic.binance.atr.period:48}")
  private int atrPeriod;

  @Scheduled(fixedDelay = 5000)
  @Override
  public void recordAtr() {
    List<BigDecimal> atrs = new ArrayList<>();
    Kline[] klineArray = klineService.queryKlineArray(
        atrSymbol, ATR_INTERVAL.code(), null, System.currentTimeMillis(), atrPeriod);
    List<BigDecimalKline> klines = Arrays.stream(klineArray)
        .map(kline -> {
          String klineJson = serializer.toJsonString(kline);
          return serializer.fromJsonString(klineJson, BigDecimalKline.class);
        })
        .toList();
    if (CollectionUtils.isEmpty(klines) || klines.size() < 2) {
      return;
    }
    klines = klines.subList(0, klines.size() - 1);

    List<BigDecimal> highs = klines.stream()
        .map(BigDecimalKline::getHighPrice)
        .toList();
    List<BigDecimal> lows = klines.stream()
        .map(BigDecimalKline::getLowPrice)
        .toList();
    List<BigDecimal> opens = klines.stream()
        .map(BigDecimalKline::getOpenPrice)
        .toList();
    List<BigDecimal> closes = klines.stream()
        .map(BigDecimalKline::getClosePrice)
        .toList();

    List<BigDecimal> trs = new ArrayList<>();
    for(int i = 0; i < klines.size(); i++) {
      BigDecimal currentHigh = highs.get(i);
      BigDecimal currentLow = lows.get(i);
      BigDecimal previousClose;
      if (i == 0) {
        previousClose = BigDecimal.ZERO;
      } else {
        previousClose = closes.get(i - 1);
      }
      BigDecimal c1 = currentHigh.subtract(currentLow);
      BigDecimal c2 = currentHigh.subtract(previousClose);
      BigDecimal c3 = currentLow.subtract(previousClose);
      BigDecimal tr = c1.max(c2).max(c3);
      trs.add(tr);
    }

    List<BigDecimal> closeRollingMeans = calculateRollingMeans(closes, atrPeriod);
    List<BigDecimal> trRollingMeans = calculateRollingMeans(trs, atrPeriod);

    for(int i = 0; i < klines.size(); i++) {
      BigDecimal closeMean = closeRollingMeans.get(i);
      BigDecimal trMean = trRollingMeans.get(i);
      BigDecimal atr = trMean.divide(closeMean.add(Constants.EPS), Constants.SCALE, RoundingMode.DOWN);
      atrs.add(atr);
    }
    log.info("atr: {}", atrs.get(atrs.size() - 1));
  }

  private static List<BigDecimal> calculateRollingMeans(List<BigDecimal> values, int windowSize) {
    List<BigDecimal> result = new ArrayList<>();

    for (int i = 0; i < values.size(); i++) {
      int start = Math.max(0, i - windowSize + 1);
      int end = i + 1;

      BigDecimal sum = BigDecimal.ZERO;
      for (int j = start; j < end; j++) {
        sum = sum.add(values.get(j));
      }
      BigDecimal mean = sum.divide(new BigDecimal(end - start), Constants.SCALE, RoundingMode.DOWN);
      result.add(mean);
    }

    return result;
  }
}
