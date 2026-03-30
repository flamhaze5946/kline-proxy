package com.zx.quant.klineproxy.client.ws.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.ws.task.ClientMonitorTask;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.util.Serializer;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class AbstractWebSocketClientTest {

  @Test
  void shouldParseCombinedMessageOnceAndDispatchPayload() throws Exception {
    CountingSerializer serializer = new CountingSerializer(new ObjectMapper());
    TestWebSocketClient client = new TestWebSocketClient(serializer);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<ParsedWebSocketMessage> handledMessage = new AtomicReference<>();
    client.addMessageHandler(parsedMessage -> {
      handledMessage.set(parsedMessage);
      latch.countDown();
      return true;
    });

    client.onReceive("{\"stream\":\"btcusdt@ticker\",\"data\":{\"e\":\"24hrTicker\",\"s\":\"BTCUSDT\",\"c\":\"1\"}}");

    assertTrue(latch.await(2, TimeUnit.SECONDS));
    ParsedWebSocketMessage parsedMessage = handledMessage.get();
    assertEquals("btcusdt@ticker", parsedMessage.stream());
    assertEquals("24hrTicker", parsedMessage.eventType());
    assertEquals("BTCUSDT", parsedMessage.payloadNode().get("s").asText());
    assertEquals(1, serializer.readTreeCalls.get());
    assertEquals(0, serializer.treeToValueCalls.get());
    assertEquals(0, serializer.fromJsonStringCalls.get());
  }

  @Test
  void shouldUpdateChannelRegisteredTopicsFromListTopicsMessage() throws Exception {
    CountingSerializer serializer = new CountingSerializer(new ObjectMapper());
    TestWebSocketClient client = new TestWebSocketClient(serializer);
    AtomicInteger handlerCallCount = new AtomicInteger(0);
    client.addMessageHandler(parsedMessage -> {
      handlerCallCount.incrementAndGet();
      return true;
    });

    client.onReceive("{\"result\":[\"btcusdt@ticker\"],\"id\":1}");

    assertTrue(awaitCondition(() -> client.getChannelRegisteredTopics().contains("btcusdt@ticker")));
    assertEquals(List.of("btcusdt@ticker"), client.getChannelRegisteredTopics());
    assertEquals(0, handlerCallCount.get());
    assertEquals(1, serializer.readTreeCalls.get());
    assertEquals(1, serializer.treeToValueCalls.get());
  }

  private boolean awaitCondition(CheckedBooleanSupplier supplier) throws Exception {
    long deadline = System.currentTimeMillis() + 2_000L;
    while (System.currentTimeMillis() < deadline) {
      if (supplier.getAsBoolean()) {
        return true;
      }
      Thread.sleep(20L);
    }
    return supplier.getAsBoolean();
  }

  @FunctionalInterface
  private interface CheckedBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  private static class CountingSerializer extends Serializer {

    private final AtomicInteger fromJsonStringCalls = new AtomicInteger(0);

    private final AtomicInteger readTreeCalls = new AtomicInteger(0);

    private final AtomicInteger treeToValueCalls = new AtomicInteger(0);

    private CountingSerializer(ObjectMapper objectMapper) {
      super(objectMapper);
    }

    @Override
    public <T> T fromJsonString(String jsonString, Class<T> clazz) {
      fromJsonStringCalls.incrementAndGet();
      return super.fromJsonString(jsonString, clazz);
    }

    @Override
    public JsonNode readTree(String jsonString) {
      readTreeCalls.incrementAndGet();
      return super.readTree(jsonString);
    }

    @Override
    public <T> T treeToValue(JsonNode jsonNode, Class<T> clazz) {
      treeToValueCalls.incrementAndGet();
      return super.treeToValue(jsonNode, clazz);
    }
  }

  private static class TestWebSocketClient extends AbstractWebSocketClient<Long> {

    private TestWebSocketClient(Serializer serializer) {
      this.serializer = serializer;
      this.clientMonitorTask = new ClientMonitorTask(this);
    }

    @Override
    protected WebSocketFrame buildSubscribeFrame(Collection<String> topics) {
      return new TextWebSocketFrame("subscribe");
    }

    @Override
    protected WebSocketFrame buildUnsubscribeFrame(Collection<String> topics) {
      return new TextWebSocketFrame("unsubscribe");
    }

    @Override
    protected WebSocketFrame buildListTopicsFrame() {
      return new TextWebSocketFrame("list");
    }

    @Override
    protected int getMaxTopicsPerTime() {
      return 10;
    }

    @Override
    protected int getMaxFramesPerSecond() {
      return 10;
    }

    @Override
    protected boolean monitorTopicMessage() {
      return true;
    }

    @Override
    protected String getUrl() {
      return "wss://stream.binance.com/ws";
    }

    @Override
    protected Long generateSubId() {
      return 1L;
    }

    @Override
    protected Long generateId() {
      return 1L;
    }
  }
}
