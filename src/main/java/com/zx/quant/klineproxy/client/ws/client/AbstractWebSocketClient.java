package com.zx.quant.klineproxy.client.ws.client;

import com.zx.quant.klineproxy.client.ws.enums.ProtocolEnum;
import com.zx.quant.klineproxy.client.ws.handler.WebSocketChannelInboundHandler;
import com.zx.quant.klineproxy.client.ws.task.MonitorTask;
import com.zx.quant.klineproxy.client.ws.task.PingTask;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * abstract webSocket client
 * @author flamhaze5946
 */
@Slf4j
public abstract class AbstractWebSocketClient<T> implements WebSocketClient {

  private static final String SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-monitor-";

  private static final String MESSAGE_EXECUTOR_GROUP_PREFIX = "websocket-handler-";

  private static final ExecutorService MESSAGE_EXECUTOR = buildMessageExecutor();

  private static final String PING = "PING";

  private static final String PONG = "PONG";

  private final Supplier<WebSocketChannelInboundHandler> handlerSupplier;

  private final List<Consumer<String>> messageHandlers;

  @Autowired
  protected Serializer serializer;

  protected T subId;

  protected volatile URI uri;

  protected Channel channel;

  protected Set<String> topics = new HashSet<>();

  protected EventLoopGroup group;

  protected MonitorTask monitorTask;

  private ScheduledExecutorService scheduledExecutorService;

  public AbstractWebSocketClient() {
    this(WebSocketChannelInboundHandler::new);
  }

  public AbstractWebSocketClient(Supplier<WebSocketChannelInboundHandler> handlerSupplier) {
    this.handlerSupplier = handlerSupplier;
    this.messageHandlers = new ArrayList<>();
  }

  public void start() {
    this.connect();
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdown();
    }
    this.monitorTask = new MonitorTask(this);
    PingTask pingTask = new PingTask(this);
    ThreadFactory scheduleThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(getScheduleExecutorGroupName());
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(scheduleThreadFactory);
    scheduledExecutorService.scheduleWithFixedDelay(monitorTask, 0,15000, TimeUnit.MILLISECONDS);
    scheduledExecutorService.scheduleWithFixedDelay(pingTask, 0,5000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void reconnect() {
    if (group != null) {
      this.group.shutdownGracefully().syncUninterruptibly();
    }
    this.group = null;
    this.connect();
    if (!alive()) {
      for (String topic : topics) {
        this.subscribeTopic(topic);
      }
    }
  }

  @Override
  public synchronized void addMessageHandler(Consumer<String> messageHandler) {
    this.messageHandlers.add(messageHandler);
  }

  @Override
  public void onReceive(String message) {
    CompletableFuture.runAsync(() -> {
      monitorTask.heartbeat();
      if (StringUtils.contains(message, PING)) {
        this.sendMessage(message.replace(PING, PONG));
        return;
      }
      if (StringUtils.contains(message, PONG)) {
        this.ping();
        return;
      }

      for (Consumer<String> messageHandler : messageHandlers) {
        messageHandler.accept(message);
      }
    }, MESSAGE_EXECUTOR);
  }

  @Override
  public void onReceiveNoHandle() {
    monitorTask.heartbeat();
  }

  @Override
  public URI uri() {
    if (uri == null) {
      synchronized (this) {
        if (uri == null) {
          uri = buildUri();
        }
      }
    }
    return uri;
  }

  @Override
  public void connect() {
    try {
      subId = generateSubId();
      WebSocketChannelInboundHandler handler = handlerSupplier.get();
      handler.init(this);

      connectWebSocket(uri(), handler);
      if (alive()) {
        handler.getHandshakeFuture().sync();
      }
    } catch (Exception e) {
      log.error("websocket client: {} start failed.", clientName(), e);
      if (group != null) {
        group.shutdownGracefully().syncUninterruptibly();
      }
    }
  }

  @Override
  public boolean alive() {
    return channel != null && channel.isActive();
  }

  @Override
  public void close() {
    monitorTask = null;
    if (scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
      scheduledExecutorService.shutdown();
    }
    if (channel != null) {
      channel.close();
    }
  }

  @Override
  public void sendMessage(String message) {
    if (!alive()) {
      log.warn("client: {} not alive, message: {} send failed.", clientName(), message);
      return;
    }
    channel.writeAndFlush(new TextWebSocketFrame(message));
    log.info("client: {} message: {} sent.", clientName(), message);
  }

  public void ping() {
    if (alive()) {
      channel.writeAndFlush(new PingWebSocketFrame());
    }
  }

  protected abstract String getUrl();

  protected abstract T generateSubId();

  protected abstract T generateId();

  protected void connectWebSocket(URI uri, WebSocketChannelInboundHandler handler) {
    try {
      String protocol = uri.getScheme();
      ProtocolEnum protocolEnum = CommonUtil.getEnumByCode(protocol, ProtocolEnum.class);
      if (protocolEnum == null) {
        throw new RuntimeException("protocol not supported.");
      }

      String host = uri.getHost();
      int port = protocolEnum.getPort();
      boolean ssl = protocolEnum.isSsl();
      final SslContext sslCtx;
      if (ssl) {
        sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
      } else {
        sslCtx = null;
      }
      if (group != null && !group.isShutdown()) {
        group.shutdownGracefully().syncUninterruptibly();
      }
      group = new NioEventLoopGroup(2);
      Bootstrap bootstrap = new Bootstrap();
      bootstrap
          .group(group)
          .channel(NioSocketChannel.class)
          .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
              ChannelPipeline pipeline = ch.pipeline();
              if (sslCtx != null) {
                pipeline.addLast(sslCtx.newHandler(ch.alloc(), host, port));
              }
              pipeline.addLast(new HttpClientCodec(), new HttpObjectAggregator(81920), handler);
            }
          });
      channel = bootstrap.connect(host, port).sync().channel();
    } catch (Exception e) {
      log.error(" websocket client: {} start error.", clientName(), e);
      if (group != null) {
        group.shutdownGracefully().syncUninterruptibly();
      }
    }
  }

  protected URI buildUri() {
    try {
      return new URI(getUrl());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }


  private String getScheduleExecutorGroupName() {
    return SCHEDULE_EXECUTOR_GROUP_PREFIX + clientName();
  }

  private static ExecutorService buildMessageExecutor() {
    ThreadFactory namedThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        MESSAGE_EXECUTOR_GROUP_PREFIX);
    return new ThreadPoolExecutor(
        2,
        20,
        1,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(1024),
        namedThreadFactory,
        new CallerRunsPolicy());
  }
}
