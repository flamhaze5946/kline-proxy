package com.zx.quant.klineproxy.client.ws.handler;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import java.net.URI;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class WebSocketChannelInboundHandlerTest {

  @Test
  void binaryFrameIsReleasedAfterSynchronousDecompression() throws Exception {
    WebSocketClient client = Mockito.mock(WebSocketClient.class);
    when(client.uri()).thenReturn(URI.create("wss://stream.binance.com/ws"));
    when(client.clientName()).thenReturn("test-client");
    WebSocketChannelInboundHandler handler = new WebSocketChannelInboundHandler();
    handler.init(client);
    String json = "{\"e\":\"kline\",\"k\":{\"x\":true}}";
    ByteArrayOutputStream compressed = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
      gzip.write(json.getBytes(StandardCharsets.UTF_8));
    }
    BinaryWebSocketFrame frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(compressed.toByteArray()));
    EmbeddedChannel channel = new EmbeddedChannel(handler);
    try {
      channel.writeInbound(frame);
      verify(client).onReceive(eq(json), anyLong(), anyLong());
      assertEquals(0, frame.refCnt(), "no extra retained buffer after SimpleChannelInboundHandler releases the frame");
    } finally {
      channel.finishAndReleaseAll();
    }
  }

  @Test
  void shouldAggregateFragmentedTextFramesBeforeHandling() {
    WebSocketClient webSocketClient = Mockito.mock(WebSocketClient.class);
    when(webSocketClient.uri()).thenReturn(URI.create("wss://stream.binance.com/ws"));
    when(webSocketClient.clientName()).thenReturn("test-client");

    WebSocketChannelInboundHandler handler = new WebSocketChannelInboundHandler();
    handler.init(webSocketClient);

    EmbeddedChannel channel = new EmbeddedChannel(
        new WebSocketFrameAggregator(1024),
        handler
    );
    channel.writeInbound(new TextWebSocketFrame(false, 0, "{\"stream\":\"btcusdt@ticker\","));
    channel.writeInbound(new ContinuationWebSocketFrame(true, 0, "\"data\":{\"price\":\"1\"}}"));

    verify(webSocketClient, times(1))
        .onReceive(eq("{\"stream\":\"btcusdt@ticker\",\"data\":{\"price\":\"1\"}}"), anyLong(), anyLong());
    channel.finishAndReleaseAll();
  }
}
