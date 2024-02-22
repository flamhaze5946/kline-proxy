package com.zx.quant.klineproxy.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * serializer
 * @author flamhaze5946
 */
public class Serializer {

  private final ObjectMapper objectMapper;

  public Serializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public String toJsonString(Object obj) {
    try {
      return objectMapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T fromJsonString(String jsonString, Class<T> clazz) {
    if (CommonUtil.isArrayMessage(jsonString) && !clazz.isArray()) {
      return null;
    }

    if (!CommonUtil.isArrayMessage(jsonString) && clazz.isArray()) {
      return null;
    }

    try {
      return objectMapper.readValue(jsonString, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
