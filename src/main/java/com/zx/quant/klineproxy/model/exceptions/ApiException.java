package com.zx.quant.klineproxy.model.exceptions;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * API exception with HTTP status and binance-style error code.
 * @author flamhaze5946
 */
@Getter
public class ApiException extends RuntimeException {

  private final HttpStatus status;

  private final int code;

  public ApiException(HttpStatus status, int code, String message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}
