package com.zx.quant.klineproxy.util;

import lombok.extern.slf4j.Slf4j;

/**
 * exception safe runnable
 * regular runnable would be closed when exception occur at scheduled executor
 * ExceptionSafeRunnable will try catch exceptions and make it could schedule running continuously
 * @author flamhaze5946
 */
@Slf4j
public class ExceptionSafeRunnable implements Runnable {

  private final Runnable target;

  public ExceptionSafeRunnable(Runnable target) {
    this.target = target;
  }

  @Override
  public void run() {
    try{
      this.target.run();
    } catch (Exception e) {
      log.error("ExceptionSafeRunnable for {} running error.", this.target, e);
    }
  }
}
