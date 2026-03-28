package com.zx.quant.klineproxy.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * binance-style error response
 * @author flamhaze5946
 */
@Data
@AllArgsConstructor
public class BinanceErrorResponse {

  private int code;

  private String msg;
}
