package com.zx.quant.klineproxy.config;

import com.zx.quant.klineproxy.model.BinanceErrorResponse;
import com.zx.quant.klineproxy.model.exceptions.ApiException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * global exception config
 * @author flamhaze5946
 */
@Slf4j
@Configuration
@RestControllerAdvice
public class GlobalExceptionConfig {

  @ExceptionHandler(ApiException.class)
  public ResponseEntity<BinanceErrorResponse> apiException(ApiException e) {
    log.warn("API exception occur.", e);
    return ResponseEntity.status(e.getStatus())
        .body(new BinanceErrorResponse(e.getCode(), e.getMessage()));
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<BinanceErrorResponse> exception(Exception e) {
    log.warn("uncaught exception occur.", e);
    return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
        .body(new BinanceErrorResponse(-1000, e.getMessage()));
  }
}
