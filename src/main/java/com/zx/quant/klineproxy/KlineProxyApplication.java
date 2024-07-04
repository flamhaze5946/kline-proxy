package com.zx.quant.klineproxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;

/**
 * kline proxy application
 * @author flamhaze5946
 */
@SpringBootApplication(exclude = {RedisAutoConfiguration.class})
public class KlineProxyApplication {

  public static void main(String[] args) {
    SpringApplication.run(KlineProxyApplication.class, args);
  }

}
