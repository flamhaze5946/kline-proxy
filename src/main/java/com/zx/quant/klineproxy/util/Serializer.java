package com.zx.quant.klineproxy.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * serializer
 * @author flamhaze5946
 */
public class Serializer {

  private static Serializer defaultSerializer;

  private final ObjectMapper objectMapper;
  private final ConcurrentHashMap<Class<?>, PayloadReaders> payloadReaders = new ConcurrentHashMap<>();

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

  /** Produce wire-ready UTF-8 once for cached HTTP responses. */
  public byte[] toJsonBytes(Object obj) {
    try {
      return objectMapper.writeValueAsBytes(obj);
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

  public JsonParser createParser(String jsonString) throws IOException {
    return objectMapper.getFactory().createParser(jsonString);
  }

  /** Decode a classified protocol payload directly, without constructing an intermediate tree. */
  public <T> T readWebSocketPayload(String raw, boolean combined, Class<T> type) {
    PayloadReaders readers = payloadReaders.computeIfAbsent(type, key -> {
      ObjectReader root = objectMapper.readerFor(key);
      return new PayloadReaders(root, root.at("/data"));
    });
    try {
      return type.cast((combined ? readers.combined() : readers.root()).readValue(raw));
    } catch (IOException error) {
      throw new RuntimeException(error);
    }
  }

  private record PayloadReaders(ObjectReader root, ObjectReader combined) { }

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
