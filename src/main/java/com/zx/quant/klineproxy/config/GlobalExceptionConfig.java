package com.zx.quant.klineproxy.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
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

  @ExceptionHandler(Exception.class)
  public String exception(Exception e) {
    log.warn("uncaught exception occur.", e);
    return e.getMessage();
  }
}
