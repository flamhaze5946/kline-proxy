package com.zx.quant.klineproxy.model;

import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * kline
 * @author flamhaze5946
 */
@Data
public abstract class Ticker<N> {

  protected String symbol;

  protected N price;

  protected Long time;

  public static Ticker<?> create(String symbol, Kline<?> kline, Long time) {
    if (kline instanceof Kline.StringKline stringKline) {
      StringTicker stringTicker = new StringTicker();
      stringTicker.setSymbol(symbol);
      stringTicker.setPrice(stringKline.getClosePrice());
      stringTicker.setTime(time);
      return stringTicker;
    } else if (kline instanceof Kline.DoubleKline doubleKline) {
      DoubleTicker doubleTicker = new DoubleTicker();
      doubleTicker.setSymbol(symbol);
      doubleTicker.setPrice(doubleKline.getClosePrice());
      doubleTicker.setTime(time);
      return doubleTicker;
    } else if(kline instanceof Kline.BigDecimalKline bigDecimalKline) {
      BigDecimalTicker bigDecimalTicker = new BigDecimalTicker();
      bigDecimalTicker.setSymbol(symbol);
      bigDecimalTicker.setPrice(bigDecimalKline.getClosePrice());
      bigDecimalTicker.setTime(time);
      return bigDecimalTicker;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class StringTicker extends Ticker<String> {
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class DoubleTicker extends Ticker<Double> {
  }

  @EqualsAndHashCode(callSuper = true)
  @NoArgsConstructor
  @Data
  public static class BigDecimalTicker extends Ticker<BigDecimal> {
  }
}
