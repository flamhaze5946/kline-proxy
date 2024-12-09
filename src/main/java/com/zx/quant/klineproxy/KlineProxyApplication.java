package com.zx.quant.klineproxy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * kline proxy application
 * @author flamhaze5946
 */
@EnableScheduling
@SpringBootApplication(exclude = {RedisAutoConfiguration.class})
public class KlineProxyApplication {

  public static void main(String[] args) {
    SpringApplication.run(KlineProxyApplication.class, args);
  }

}
