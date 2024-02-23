package com.zx.quant.klineproxy.config;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
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
  public ObjectMapper objectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion(Include.NON_NULL);
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return objectMapper;
  }

  @Bean
  public Serializer serializer(ObjectMapper objectMapper) {
    Serializer serializer = new Serializer(objectMapper);
    Serializer.setDefault(serializer);
    return serializer;
  }
}
