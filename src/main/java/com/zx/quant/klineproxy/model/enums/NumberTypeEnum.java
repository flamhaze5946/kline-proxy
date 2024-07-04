package com.zx.quant.klineproxy.model.enums;

import com.zx.quant.klineproxy.model.EventKline;
import com.zx.quant.klineproxy.model.EventKline.BigDecimalEventKline;
import com.zx.quant.klineproxy.model.EventKline.DoubleEventKline;
import com.zx.quant.klineproxy.model.EventKline.FloatEventKline;
import com.zx.quant.klineproxy.model.EventKline.StringEventKline;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import com.zx.quant.klineproxy.model.EventKlineEvent.BigDecimalEventKlineEvent;
import com.zx.quant.klineproxy.model.EventKlineEvent.DoubleEventKlineEvent;
import com.zx.quant.klineproxy.model.EventKlineEvent.FloatEventKlineEvent;
import com.zx.quant.klineproxy.model.EventKlineEvent.StringEventKlineEvent;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.BigDecimalKline;
import com.zx.quant.klineproxy.model.Kline.DoubleKline;
import com.zx.quant.klineproxy.model.Kline.FloatKline;
import com.zx.quant.klineproxy.model.Kline.StringKline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker.BigDecimalTicker;
import com.zx.quant.klineproxy.model.Ticker.DoubleTicker;
import com.zx.quant.klineproxy.model.Ticker.FloatTicker;
import com.zx.quant.klineproxy.model.Ticker.StringTicker;
import com.zx.quant.klineproxy.util.BaseEnum;

/**
 * number type enum
 * @author flamhaze5946
 */
public enum NumberTypeEnum implements BaseEnum {

  STRING("string", StringKline.class,
      StringEventKline.class, StringEventKlineEvent.class,
      StringTicker.class, "STRING"),

  FLOAT("float", FloatKline.class,
      FloatEventKline.class, FloatEventKlineEvent.class,
      FloatTicker.class, "FLOAT"),

  DOUBLE("double", DoubleKline.class,
      DoubleEventKline.class, DoubleEventKlineEvent.class,
      DoubleTicker.class, "DOUBLE"),

  BIG_DECIMAL("bigDecimal", BigDecimalKline.class,
      BigDecimalEventKline.class, BigDecimalEventKlineEvent.class,
      BigDecimalTicker.class, "BIG_DECIMAL"),
  ;

  private final String code;

  private final Class<? extends Kline> klineClass;

  private final Class<? extends EventKline> eventKlineClass;

  private final Class<? extends EventKlineEvent<?, ?>> eventKlineEventClass;

  private final Class<? extends Ticker<?>> tickerClass;

  private final String description;

  NumberTypeEnum(String code, Class<? extends Kline> klineClass, Class<? extends EventKline> eventKlineClass, Class<? extends EventKlineEvent<?, ?>> eventKlineEventClass, Class<? extends Ticker<?>> tickerClass, String description) {
    this.code = code;
    this.klineClass = klineClass;
    this.eventKlineClass = eventKlineClass;
    this.eventKlineEventClass = eventKlineEventClass;
    this.tickerClass = tickerClass;
    this.description = description;
  }

  @Override
  public String code() {
    return code;
  }

  public Class<? extends Kline> klineClass() {
    return klineClass;
  }

  public Class<? extends EventKline> eventKlineClass() {
    return eventKlineClass;
  }

  public Class<? extends EventKlineEvent<?, ?>> eventKlineEventClass() {
    return eventKlineEventClass;
  }

  public Class<? extends Ticker<?>> tickerClass() {
    return tickerClass;
  }

  @Override
  public String description() {
    return description;
  }
}
