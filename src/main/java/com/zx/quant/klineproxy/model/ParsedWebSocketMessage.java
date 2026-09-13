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
    WebSocketMessageTiming timing,
    long receiveSequence,
    KlineDispatchMetadata dispatchMetadata) {

  public ParsedWebSocketMessage(String rawMessage, JsonNode rootNode, JsonNode payloadNode,
      String stream, String eventType, WebSocketMessageTiming timing, long receiveSequence) {
    this(rawMessage, rootNode, payloadNode, stream, eventType, timing, receiveSequence, null);
  }

  public ParsedWebSocketMessage(String rawMessage, JsonNode rootNode, JsonNode payloadNode,
      String stream, String eventType, WebSocketMessageTiming timing) {
    this(rawMessage, rootNode, payloadNode, stream, eventType, timing, 0L, null);
  }

  public ParsedWebSocketMessage(String rawMessage, JsonNode rootNode, JsonNode payloadNode,
      String stream, String eventType) {
    this(rawMessage, rootNode, payloadNode, stream, eventType, null, 0L, null);
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
