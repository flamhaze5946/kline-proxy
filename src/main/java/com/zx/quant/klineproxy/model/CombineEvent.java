package com.zx.quant.klineproxy.model;

import lombok.Data;

/**
 * combine event
 * @author flamhaze5946
 */
@Data
public class CombineEvent {

  private String stream;

  private Object data;
}
