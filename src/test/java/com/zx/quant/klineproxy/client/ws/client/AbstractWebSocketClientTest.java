package com.zx.quant.klineproxy.client.ws.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.ws.task.ClientMonitorTask;
import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import com.zx.quant.klineproxy.client.ws.dispatch.KlineMessageDispatcher;
import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.util.Serializer;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.channel.embedded.EmbeddedChannel;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class AbstractWebSocketClientTest {

  @Test
  void classifiedFormingSkipsCloseDiagnosticsWhileFinalKeepsReceiptTiming() throws Exception {
    TestWebSocketClient client = new TestWebSocketClient(new Serializer(new ObjectMapper()));
    try (var dispatcher = new KlineMessageDispatcher(new KlineIngressProperties())) {
      ReflectionTestUtils.setField(client, "klineMessageDispatcher", dispatcher);
      var series = new KlineDispatchMetadata.Series("future", "BTCUSDT", "1h");
      client.setKlineMessageClassifier(raw -> new KlineDispatchMetadata(series, 0,
          raw.contains("true"), 10, 100L, "btcusdt@kline_1h"));
      CountDownLatch formingHandled = new CountDownLatch(1);
      CountDownLatch finalHandled = new CountDownLatch(1);
      AtomicReference<ParsedWebSocketMessage> forming = new AtomicReference<>();
      AtomicReference<ParsedWebSocketMessage> closed = new AtomicReference<>();
      client.addMessageHandler(message -> {
        if (message.dispatchMetadata().closed()) {
          closed.set(message);
          finalHandled.countDown();
        } else {
          forming.set(message);
          formingHandled.countDown();
        }
        return true;
      });
      client.onReceive("{\"e\":\"kline\",\"k\":{\"x\":false}}");
      assertTrue(formingHandled.await(1, TimeUnit.SECONDS));
      assertTrue(forming.get().timing() == null);
      long frameMillis = System.currentTimeMillis();
      long frameNanos = System.nanoTime();
      client.onReceive("{\"e\":\"kline\",\"k\":{\"x\":true}}", frameMillis, frameNanos);
      assertTrue(finalHandled.await(1, TimeUnit.SECONDS));
      assertEquals(frameMillis, closed.get().timing().getReceivedAtMillis());
      assertEquals(frameNanos, closed.get().timing().getReceivedAtNanos());
      assertTrue(closed.get().receiveSequence() > forming.get().receiveSequence());
    }
  }

  @Test
  void closeReleasesQueuedFramesAndPreventsAutomaticReconnect() {
    TestWebSocketClient client = new TestWebSocketClient(new Serializer(new ObjectMapper()));
    client.channel = new EmbeddedChannel();
    TextWebSocketFrame queued = new TextWebSocketFrame("queued");
    client.sendData(queued);
    assertEquals(1, queued.refCnt());
    client.close();
    assertEquals(0, queued.refCnt());
    TextWebSocketFrame rejected = new TextWebSocketFrame("rejected");
    client.sendData(rejected);
    assertEquals(0, rejected.refCnt());
    TextWebSocketFrame rejectedDirect = new TextWebSocketFrame("rejected direct");
    client.sendData0(rejectedDirect);
    assertEquals(0, rejectedDirect.refCnt());
    client.reconnect();
    client.connect();
    client.start();
    assertTrue(client.inboundHandler == null, "a closed client must not reconnect");
  }

  @Test
  void genericDrainIncludesTheRunningHandlerEvenWhenTheQueueIsEmpty() throws Exception {
    TestWebSocketClient client = new TestWebSocketClient(new Serializer(new ObjectMapper()));
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    client.addMessageHandler(message -> {
      started.countDown();
      try {
        release.await(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return true;
    });
    try {
      client.onReceive("{\"e\":\"kline\"}");
      assertTrue(started.await(1, TimeUnit.SECONDS));
      assertTrue(!AbstractWebSocketClient.awaitGenericMessageTasks(Duration.ofMillis(1)));
    } finally {
      release.countDown();
    }
    assertTrue(AbstractWebSocketClient.awaitGenericMessageTasks(Duration.ofSeconds(1)));
  }

  @Test
  void symbolOrPayloadContainingPingPongMustRemainMarketData() throws Exception {
    TestWebSocketClient client = new TestWebSocketClient(new Serializer(new ObjectMapper()));
    CountDownLatch handled = new CountDownLatch(2);
    client.addMessageHandler(message -> { handled.countDown(); return true; });
    client.onReceive("{\"e\":\"kline\",\"s\":\"PINGUSDT\",\"k\":{\"x\":true}}");
    client.onReceive("{\"e\":\"kline\",\"s\":\"PONGUSDT\",\"k\":{\"x\":true}}");
    assertTrue(handled.await(2, TimeUnit.SECONDS));
  }

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
    assertTrue(parsedMessage.timing().getJsonParsedAtNanos() >= parsedMessage.timing().getHandlerStartedAtNanos());
    assertTrue(parsedMessage.timing().getHandlerStartedAtNanos() >= parsedMessage.timing().getEnqueuedAtNanos());
  }

  @Test
  void timestampsPreserveFrameReceiptWhileMessageWaitsForWorker() throws Exception {
    TestWebSocketClient client = new TestWebSocketClient(new Serializer(new ObjectMapper()));
    ThreadPoolExecutor executor = (ThreadPoolExecutor) ReflectionTestUtils.getField(
        AbstractWebSocketClient.class, "MESSAGE_EXECUTOR");
    CountDownLatch occupied = new CountDownLatch(executor.getCorePoolSize());
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch handled = new CountDownLatch(1);
    AtomicReference<ParsedWebSocketMessage> received = new AtomicReference<>();
    client.addMessageHandler(message -> {
      received.set(message);
      handled.countDown();
      return true;
    });
    try {
      for (int i = 0; i < executor.getCorePoolSize(); i++) {
        executor.execute(() -> {
          occupied.countDown();
          try {
            release.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        });
      }
      assertTrue(occupied.await(2, TimeUnit.SECONDS));
      long frameNanos = System.nanoTime();
      long frameMillis = System.currentTimeMillis();
      client.onReceive("{\"e\":\"continuous_kline\",\"k\":{\"x\":true}}", frameMillis, frameNanos);
      assertEquals(1L, handled.getCount(), "message must still be queued");
      long releasedAt = System.nanoTime();
      release.countDown();
      assertTrue(handled.await(2, TimeUnit.SECONDS));
      var timing = received.get().timing();
      assertEquals(frameMillis, timing.getReceivedAtMillis());
      assertEquals(frameNanos, timing.getReceivedAtNanos());
      assertTrue(timing.getEnqueuedAtNanos() <= releasedAt);
      assertTrue(timing.getHandlerStartedAtNanos() >= releasedAt);
      assertTrue(timing.getPoolSize() >= executor.getCorePoolSize());
      assertTrue(timing.getActiveThreads() >= 1);
    } finally {
      release.countDown();
    }
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
