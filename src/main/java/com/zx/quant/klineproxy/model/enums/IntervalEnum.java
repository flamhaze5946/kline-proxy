package com.zx.quant.klineproxy.model.enums;

import com.zx.quant.klineproxy.util.BaseEnum;

/**
 * interval enum
 * @author flamhaze5946
 */
public enum IntervalEnum implements BaseEnum {

  ONE_MINUTE("1m", 1, 1000 * 60, "1 minute"),

  THREE_MINUTE("3m", 2, 1000 * 60 * 3, "3 minute"),

  FIVE_MINUTE("5m", 3, 1000 * 60 * 5, "5 minute"),

  FIFTEEN_MINUTE("15m", 4, 1000 * 60 * 15, "15 minute"),

  THIRTY_MINUTE("30m", 5, 1000 * 60 * 30, "30 minute"),

  ONE_HOUR("1h", 6, 1000 * 60 * 60, "1 hour"),

  TWO_HOUR("2h", 7, 1000 * 60 * 60 * 2, "2 hour"),

  FOUR_HOUR("4h", 8, 1000 * 60 * 60 * 4, "4 hour"),

  SIX_HOUR("6h", 9, 1000 * 60 * 60 * 6, "6 hour"),

  EIGHT_HOUR("8h", 10, 1000 * 60 * 60 * 8, "8 hour"),

  TWELVE_HOUR("12h", 11, 1000 * 60 * 60 * 12, "12 hour"),

  ONE_DAY("1d", 12, 1000 * 60 * 60 * 24, "1 day"),

  THREE_DAY("3d", 13, 1000 * 60 * 60 * 24 * 3, "3 day"),

  ONE_WEEK("1w", 14, 1000 * 60 * 60 * 24 * 7, "1 week"),

  ONE_MONTH("1M", 15, 1000 * 60 * 60 * 24 * 30L, "1 month"),
  ;

  private final String code;

  private final int order;

  private final long mills;

  private final String description;

  IntervalEnum(String code, int order, long mills, String description) {
    this.code = code;
    this.order = order;
    this.mills = mills;
    this.description = description;
  }

  @Override
  public String code() {
    return code;
  }

  @Override
  public String description() {
    return description;
  }

  public int getOrder() {
    return order;
  }

  public long getMills() {
    return mills;
  }
}
