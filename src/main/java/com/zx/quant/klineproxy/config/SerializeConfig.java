package com.zx.quant.klineproxy.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.util.Serializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * serialize config
 * @author flamhaze5946
 */
@Configuration
public class SerializeConfig {

  @Bean
  public Serializer serializer(ObjectMapper objectMapper) {
    return new Serializer(objectMapper);
  }
}
