package com.zx.quant.klineproxy.util;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.*;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;

/** Numeric-mode-preserving zero bars. Current placeholders are read-only views. */
public final class KlineFill {
  private KlineFill() { }

  public static Kline currentAfter(Kline previous, boolean websocketFinal, IntervalEnum interval, long now) {
    if (previous == null || !websocketFinal) {
      return null;
    }
    try {
      long open = Math.addExact(previous.getCloseTime(), 1L);
      long end = interval == IntervalEnum.ONE_MONTH
          ? Instant.ofEpochMilli(open).atZone(ZoneOffset.UTC).plusMonths(1).toInstant().toEpochMilli()
          : Math.addExact(open, interval.getMills());
      // Only the immediately following period: never chain synthetic history over a gap.
      return open <= now && now < end ? zeroVolume(previous, open, end - 1) : null;
    } catch (ArithmeticException | DateTimeException invalidTime) {
      return null;
    }
  }

  public static Kline zeroVolume(Kline previousKline, long openTime, long closeTime) {
    if (previousKline instanceof StringKline stringKline) {
      StringKline insertKline = stringKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(stringKline.getClosePrice());
      insertKline.setLowPrice(stringKline.getClosePrice());
      insertKline.setOpenPrice(stringKline.getClosePrice());
      insertKline.setClosePrice(stringKline.getClosePrice());
      insertKline.setVolume("0");
      insertKline.setQuoteVolume("0");
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume("0");
      insertKline.setActiveBuyQuoteVolume("0");
      return insertKline;
    } else if (previousKline instanceof FloatKline floatKline) {
      FloatKline insertKline = floatKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(floatKline.getClosePrice());
      insertKline.setLowPrice(floatKline.getClosePrice());
      insertKline.setOpenPrice(floatKline.getClosePrice());
      insertKline.setClosePrice(floatKline.getClosePrice());
      insertKline.setVolume(0);
      insertKline.setQuoteVolume(0);
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume(0);
      insertKline.setActiveBuyQuoteVolume(0);
      return insertKline;
    } else if (previousKline instanceof DoubleKline doubleKline) {
      DoubleKline insertKline = doubleKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(doubleKline.getClosePrice());
      insertKline.setLowPrice(doubleKline.getClosePrice());
      insertKline.setOpenPrice(doubleKline.getClosePrice());
      insertKline.setClosePrice(doubleKline.getClosePrice());
      insertKline.setVolume(0);
      insertKline.setQuoteVolume(0);
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume(0);
      insertKline.setActiveBuyQuoteVolume(0);
      return insertKline;
    } else if (previousKline instanceof BigDecimalKline bigDecimalKline) {
      BigDecimalKline insertKline = bigDecimalKline.deepCopy();
      insertKline.setOpenTime(openTime);
      insertKline.setCloseTime(closeTime);
      insertKline.setHighPrice(bigDecimalKline.getClosePrice());
      insertKline.setLowPrice(bigDecimalKline.getClosePrice());
      insertKline.setOpenPrice(bigDecimalKline.getClosePrice());
      insertKline.setClosePrice(bigDecimalKline.getClosePrice());
      insertKline.setVolume(BigDecimal.ZERO);
      insertKline.setQuoteVolume(BigDecimal.ZERO);
      insertKline.setTradeNum(0);
      insertKline.setActiveBuyVolume(BigDecimal.ZERO);
      insertKline.setActiveBuyQuoteVolume(BigDecimal.ZERO);
      return insertKline;
    } else {
      throw new UnsupportedOperationException();
    }
  }

}
