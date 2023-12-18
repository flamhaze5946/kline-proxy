package com.zx.quant.klineproxy.model;

import java.math.BigDecimal;
import lombok.Data;

/**
 * future premium index
 * @author flamhaze5946
 */
@Data
public class FuturePremiumIndex {

  private String symbol;

  private BigDecimal markPrice;

  private BigDecimal indexPrice;

  private BigDecimal estimatedSettlePrice;

  private BigDecimal lastFundingRate;

  private Long nextFundingTime;

  private BigDecimal interestRate;

  private Long time;
}
