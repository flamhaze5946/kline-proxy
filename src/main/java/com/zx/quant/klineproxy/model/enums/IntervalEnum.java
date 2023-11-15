package com.zx.quant.klineproxy.model.enums;

import com.zx.quant.klineproxy.util.BaseEnum;

/**
 * interval enum
 * @author flamhaze5946
 */
public enum IntervalEnum implements BaseEnum {

  ONE_MINUTE("1m", 1, 1000 * 60, "1 minute"),

  ONE_HOUR("1h", 2, 1000 * 60 * 60, "1 hour"),

  ONE_DAY("1d", 3, 1000 * 60 * 60 * 24, "1 day")
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
