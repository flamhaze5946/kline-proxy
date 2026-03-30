package com.zx.quant.klineproxy.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * serializer
 * @author flamhaze5946
 */
public class Serializer {

  private static Serializer defaultSerializer;

  private final ObjectMapper objectMapper;

  public Serializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public static Serializer getDefault() {
    return defaultSerializer;
  }

  public static void setDefault(Serializer serializer) {
    defaultSerializer = serializer;
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

  public JsonNode readTree(String jsonString) {
    try {
      return objectMapper.readTree(jsonString);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> T treeToValue(JsonNode jsonNode, Class<T> clazz) {
    if (jsonNode == null || jsonNode.isNull()) {
      return null;
    }
    if (jsonNode.isArray() && !clazz.isArray()) {
      return null;
    }
    if (!jsonNode.isArray() && clazz.isArray()) {
      return null;
    }
    try {
      return objectMapper.treeToValue(jsonNode, clazz);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
