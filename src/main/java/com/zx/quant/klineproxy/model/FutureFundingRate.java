package com.zx.quant.klineproxy.model;

import java.math.BigDecimal;
import lombok.Data;

/**
 * future funding rate
 * @author flamhaze5946
 */
@Data
public class FutureFundingRate {

  private String symbol;

  private Long fundingTime;

  private BigDecimal fundingRate;

  private BigDecimal markPrice;
}
