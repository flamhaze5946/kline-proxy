package com.zx.quant.klineproxy.client.ws.handler;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.util.CommonUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * websocket channel inbound handler
 * @param <T> message type
 * @author flamhaze5946
 */
@Slf4j
public class WebSocketChannelInboundHandler extends SimpleChannelInboundHandler<Object> {

  protected WebSocketClient webSocketClient;

  protected WebSocketClientHandshaker handShaker;

  protected ChannelPromise handshakeFuture;

  public void init(WebSocketClient webSocketClient) {
    URI uri = webSocketClient.uri();
    WebSocketClientHandshaker handShaker = WebSocketClientHandshakerFactory
        .newHandshaker(uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders(), 10485760);
    this.webSocketClient = webSocketClient;
    this.handShaker = handShaker;
  }

  public ChannelPromise getHandshakeFuture() {
    return handshakeFuture;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    handshakeFuture = ctx.newPromise();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    handShaker.handshake(ctx.channel());
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    submitReconnect(ctx, "disconnected");
    super.channelInactive(ctx);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    log.warn("websocket client: {} exception occur.", webSocketClient.clientName(), cause);
    if (!handshakeFuture.isDone()) {
      handshakeFuture.tryFailure(cause);
    }
    ctx.close();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
    Channel channel = ctx.channel();
    if (msg instanceof FullHttpResponse fullHttpMsg) {
      if (!handShaker.isHandshakeComplete()) {
          try {
            handShaker.finishHandshake(channel, fullHttpMsg);
            handshakeFuture.setSuccess();
          } catch (Exception e) {
            submitReconnect(ctx, "handshake failed");
          }
      }
    } else if (msg instanceof WebSocketFrame frame) {
      if (frame instanceof BinaryWebSocketFrame binaryWebSocketFrame) {
        webSocketClient.onReceive(decodeByteBuf(binaryWebSocketFrame.content().retain()));
      } else if (frame instanceof TextWebSocketFrame textWebSocketFrame) {
        webSocketClient.onReceive(textWebSocketFrame.text());
      } else if (frame instanceof PingWebSocketFrame) {
        webSocketClient.onReceiveNoHandle();
        webSocketClient.sendData(new PongWebSocketFrame(frame.content().retain()));
      } else if (frame instanceof PongWebSocketFrame) {
        webSocketClient.onReceiveNoHandle();
        if (log.isDebugEnabled()) {
          log.debug("websocket client: {} receive pong!", webSocketClient.clientName());
        }
      } else if (frame instanceof CloseWebSocketFrame) {
        webSocketClient.onReceiveNoHandle();
        log.info("websocket client: {} receive close.", webSocketClient.clientName());
        channel.close();
      } else {
        webSocketClient.onReceiveNoHandle();
        log.debug("websocket client: {} receive unknown message: {}!", webSocketClient.clientName(), frame.content().toString());
      }
    }
  }

  private void submitReconnect(ChannelHandlerContext ctx, String reason) {
    final EventLoop eventLoop = ctx.channel().eventLoop();
    eventLoop.schedule(() -> {
      log.warn("websocket client: {} {}, reconnect.", reason, webSocketClient.clientName());
      webSocketClient.reconnect();
    }, 5L, TimeUnit.SECONDS);
  }

  private String decodeByteBuf(ByteBuf buf) throws Exception {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    // gzip 解压
    bytes = CommonUtil.decompress(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
