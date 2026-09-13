package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * parsed websocket message
 * @author flamhaze5946
 */
public record ParsedWebSocketMessage(
    String rawMessage,
    JsonNode rootNode,
    JsonNode payloadNode,
    String stream,
    String eventType,
    WebSocketMessageTiming timing) {

  public ParsedWebSocketMessage(String rawMessage, JsonNode rootNode, JsonNode payloadNode,
      String stream, String eventType) {
    this(rawMessage, rootNode, payloadNode, stream, eventType, null);
  }

  public boolean combined() {
    return stream != null && !stream.isBlank();
  }

  public boolean payloadArray() {
    return payloadNode != null && payloadNode.isArray();
  }

  public boolean payloadObject() {
    return payloadNode != null && payloadNode.isObject();
  }
}
