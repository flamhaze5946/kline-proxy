package com.zx.quant.klineproxy.model;

import lombok.Data;

/**
 * event kline
 * @author flamhaze5946
 */
@Data
public class WebsocketResponse {

  private String result;

  private Long id;
}
