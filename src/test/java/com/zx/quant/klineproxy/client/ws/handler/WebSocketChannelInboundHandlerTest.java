package com.zx.quant.klineproxy.client.ws.handler;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class WebSocketChannelInboundHandlerTest {

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
        .onReceive("{\"stream\":\"btcusdt@ticker\",\"data\":{\"price\":\"1\"}}");
    channel.finishAndReleaseAll();
  }
}
