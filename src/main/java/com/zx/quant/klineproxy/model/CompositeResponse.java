package com.zx.quant.klineproxy.model;

import lombok.Data;

/**
 * composite response
 * @author flamhaze5946
 */
@Data
public class CompositeResponse<T> {

  private String code;

  private String message;

  private String messageDetail;

  private T data;

  private Boolean success;
}
