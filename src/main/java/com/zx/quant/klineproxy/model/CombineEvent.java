package com.zx.quant.klineproxy.model;

import lombok.Data;

/**
 * combine event
 * @author flamhaze5946
 */
@Data
public class CombineEvent<T> {

  private String stream;

  private T data;
}
