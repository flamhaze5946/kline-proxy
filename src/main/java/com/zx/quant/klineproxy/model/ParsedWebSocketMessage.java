package com.zx.quant.klineproxy.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Supplier;

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
    KlineDispatchMetadata dispatchMetadata,
    Supplier<JsonNode> lazyRoot) {

  public ParsedWebSocketMessage(String rawMessage, JsonNode rootNode, JsonNode payloadNode,
      String stream, String eventType, WebSocketMessageTiming timing, long receiveSequence,
      KlineDispatchMetadata dispatchMetadata) {
    this(rawMessage, rootNode, payloadNode, stream, eventType, timing, receiveSequence,
        dispatchMetadata, null);
  }

  public static ParsedWebSocketMessage classified(String raw, WebSocketMessageTiming timing,
      long sequence, KlineDispatchMetadata metadata, Supplier<JsonNode> fallback) {
    return new ParsedWebSocketMessage(raw, null, null, metadata.stream(), metadata.eventType(),
        timing, sequence, metadata, new MemoizedTree(fallback));
  }

  public boolean directPayload() {
    return lazyRoot != null;
  }

  @Override
  public JsonNode rootNode() {
    return lazyRoot != null ? lazyRoot.get() : rootNode;
  }

  @Override
  public JsonNode payloadNode() {
    if (lazyRoot == null) {
      return payloadNode;
    }
    JsonNode root = rootNode();
    JsonNode data = combined() && root != null ? root.get("data") : null;
    return data != null && !data.isNull() ? data : root;
  }

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
    return !directPayload() && payloadNode != null && payloadNode.isArray();
  }

  public boolean payloadObject() {
    return directPayload() || payloadNode != null && payloadNode.isObject();
  }

  /** Generic extension handlers retain their existing tree API; the normal kline path never uses it. */
  private static final class MemoizedTree implements Supplier<JsonNode> {
    private final Supplier<JsonNode> source;
    private volatile JsonNode value;

    private MemoizedTree(Supplier<JsonNode> source) {
      this.source = source;
    }

    @Override
    public JsonNode get() {
      JsonNode found = value;
      if (found == null) {
        synchronized (this) {
          found = value;
          if (found == null) {
            value = found = source.get();
          }
        }
      }
      return found;
    }
  }
}
