package com.zx.quant.klineproxy.client.ws.client;

import com.google.common.collect.Sets;
import com.zx.quant.klineproxy.client.ws.enums.ProtocolEnum;
import com.zx.quant.klineproxy.client.ws.handler.WebSocketChannelInboundHandler;
import com.zx.quant.klineproxy.client.ws.task.ClientMonitorTask;
import com.zx.quant.klineproxy.client.ws.task.PingTask;
import com.zx.quant.klineproxy.client.ws.task.TopicMonitorTask;
import com.zx.quant.klineproxy.client.ws.task.TopicsSubscribeTask;
import com.zx.quant.klineproxy.client.ws.task.TopicsUnsubscribeTask;
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
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
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

  private static final String CLIENT_NAME_SEP = "-";

  private static final String MONITOR_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-monitor";

  private static final String SUBSCRIBE_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-subscribe";

  private static final String MESSAGE_EXECUTOR_GROUP_PREFIX = "websocket-message-handler";

  private static final ExecutorService MESSAGE_EXECUTOR = buildMessageExecutor();

  private static final String PING = "PING";

  private static final String PONG = "PONG";

  private final Supplier<WebSocketChannelInboundHandler> handlerSupplier;

  private final List<Consumer<String>> messageHandlers;

  private final List<Function<String, String>> messageTopicExtractors;

  private final Queue<String> candidateSubscribeTopics;

  private final Queue<String> candidateUnsubscribeTopics;

  private final Queue<WebSocketFrame> webSocketFrames;

  private final Map<String, TopicMonitorTask> topicMonitorTaskMap;

  protected final Set<String> registeredTopics;

  protected final int clientNumber;

  @Autowired
  protected Serializer serializer;

  protected T subId;

  protected volatile URI uri;

  protected Channel channel;

  protected EventLoopGroup group;

  protected ClientMonitorTask clientMonitorTask;

  protected PingTask pingTask;

  protected TopicsSubscribeTask subscribeTask;

  protected TopicsUnsubscribeTask unsubscribeTask;

  private ScheduledExecutorService monitorScheduler;

  private ScheduledExecutorService subscribeScheduler;

  private String clientName;

  public AbstractWebSocketClient() {
    this(1);
  }

  public AbstractWebSocketClient(int clientNumber) {
    this(clientNumber, WebSocketChannelInboundHandler::new);
  }

  public AbstractWebSocketClient(int clientNumber, Supplier<WebSocketChannelInboundHandler> handlerSupplier) {
    this.clientNumber = clientNumber;
    this.handlerSupplier = handlerSupplier;
    this.registeredTopics = new ConcurrentSkipListSet<>();
    this.messageHandlers = new ArrayList<>();
    this.messageTopicExtractors = new ArrayList<>();
    this.candidateSubscribeTopics = new LinkedBlockingQueue<>();
    this.candidateUnsubscribeTopics = new LinkedBlockingQueue<>();
    this.webSocketFrames = new LinkedBlockingQueue<>();
    this.topicMonitorTaskMap = new ConcurrentHashMap<>();
  }

  private void executeSubscribeTask(Collection<String> topics) {
    if (!alive()) {
      log.warn("websocket client: {} not alive when subscribe topics", clientName());
      throw new RuntimeException("websocket client not alive when subscribe topics");
    }
    subscribeTopics0(topics);
    registeredTopics.addAll(topics);
    if (monitorTopicMessage()) {
      startTopicMessageMonitors(topics);
    }
  }

  private void executeUnsubscribeTask(Collection<String> topics) {
    if (!alive()) {
      log.warn("websocket client: {} not alive when unsubscribe topics", clientName());
      throw new RuntimeException("websocket client not alive when unsubscribe topics");
    }
    unsubscribeTopics0(topics);
    registeredTopics.removeAll(Sets.newHashSet(topics));
    if (monitorTopicMessage()) {
      stopTopicMessageMonitors(topics);
    }
  }

  public void start() {
    if (monitorScheduler != null) {
      monitorScheduler.shutdown();
    }
    if (subscribeScheduler != null) {
      subscribeScheduler.shutdown();
    }

    this.webSocketFrames.clear();
    this.candidateSubscribeTopics.clear();
    this.candidateUnsubscribeTopics.clear();
    this.topicMonitorTaskMap.clear();
    this.registeredTopics.clear();

    this.connect();
    this.clientMonitorTask = new ClientMonitorTask(this);
    this.pingTask = new PingTask(this);
    monitorScheduler = buildMonitorScheduler();
    monitorScheduler.scheduleWithFixedDelay(clientMonitorTask, 0,1000, TimeUnit.MILLISECONDS);
    monitorScheduler.scheduleWithFixedDelay(pingTask, 0,1000 * 60, TimeUnit.MILLISECONDS);

    this.subscribeTask = new TopicsSubscribeTask(this.candidateSubscribeTopics, getMaxTopicsPerTime(), this::executeSubscribeTask);
    this.unsubscribeTask = new TopicsUnsubscribeTask(this.candidateUnsubscribeTopics, getMaxTopicsPerTime(), this::executeUnsubscribeTask);
    subscribeScheduler = buildSubscribeScheduler();
    subscribeScheduler.scheduleWithFixedDelay(subscribeTask, 0, 1000, TimeUnit.MILLISECONDS);
    subscribeScheduler.scheduleWithFixedDelay(unsubscribeTask, 0, 1000, TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void addMessageHandler(Consumer<String> messageHandler) {
    this.messageHandlers.add(messageHandler);
  }

  @Override
  public void addMessageTopicExtractorHandler(Function<String, String> messageTopicExtractor) {
    this.messageTopicExtractors.add(messageTopicExtractor);
  }

  @Override
  public void onReceive(String message) {
    clientMonitorTask.heartbeat();
    heartbeatTopic(message);
    CompletableFuture.runAsync(() -> {
      if (StringUtils.contains(message, PING)) {
        this.sendMessage(message.replace(PING, PONG));
        return;
      }
      if (StringUtils.contains(message, PONG)) {
        return;
      }

      for (Consumer<String> messageHandler : messageHandlers) {
        messageHandler.accept(message);
      }
    }, MESSAGE_EXECUTOR);
  }

  @Override
  public void onReceiveNoHandle() {
    clientMonitorTask.heartbeat();
  }

  @Override
  public synchronized void subscribeTopics(Collection<String> topics) {
    candidateSubscribeTopics.addAll(topics);
  }

  @Override
  public synchronized void unsubscribeTopics(Collection<String> topics) {
    candidateUnsubscribeTopics.addAll(topics);
  }

  protected abstract void subscribeTopics0(Collection<String> topics);

  protected abstract void unsubscribeTopics0(Collection<String> topics);

  protected abstract int getMaxTopicsPerTime();

  protected abstract boolean monitorTopicMessage();

  @Override
  public List<String> getSubscribedTopics() {
    return List.copyOf(registeredTopics);
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
        this.subscribeTopics(registeredTopics);
      } else {
        log.warn("websocket client: {} not alived when connect.", clientName());
      }
    } catch (Exception e) {
      log.error("websocket client: {} start failed.", clientName(), e);
      if (group != null) {
        group.shutdownGracefully().addListener(event ->
            log.info("websocket client: {} event group: {} has been shutting down, cause by connect error.", clientName(), group));
      }
    }
  }

  @Override
  public void reconnect() {
    try{
      log.info("websocket client: {} start to reconnect.", clientName());
      this.connect();
    } catch (Exception e) {
      log.warn("websocket client: {} reconnect failed.", clientName(), e);
    } finally{
      log.warn("websocket client: {} reconnect complete.", clientName());
    }
  }

  @Override
  public boolean alive() {
    return channel != null && channel.isActive();
  }

  @Override
  public void close() {
    clientMonitorTask = null;
    if (monitorScheduler != null && !monitorScheduler.isShutdown()) {
      monitorScheduler.shutdown();
    }
    if (channel != null) {
      channel.close();
    }
  }

  @Override
  public void sendData(WebSocketFrame frame) {
    if (!alive()) {
      log.warn("client: {} not alive, data: {} send failed.", clientName(), frame);
      return;
    }
    channel.writeAndFlush(frame);
    String frameLog;
    if (frame instanceof TextWebSocketFrame textWebSocketFrame) {
      frameLog = textWebSocketFrame.text();
    } else {
      frameLog = frame.getClass().getSimpleName();
    }
    log.info("client: {} data: {} sent.", clientName(), frameLog);
  }

  @Override
  public String clientName() {
    if (StringUtils.isBlank(clientName)) {
      clientName = String.join(CLIENT_NAME_SEP, getClass().getSimpleName(), String.valueOf(clientNumber));
    }
    return clientName;
  }

  @Override
  public void ping() {
    if (alive()) {
      channel.writeAndFlush(new PingWebSocketFrame());
    } else {
      log.warn("websocket client: {} not alived when ping.", clientName());
    }
  }

  @Override
  public void pong() {
    if (alive()) {
      channel.writeAndFlush(new PongWebSocketFrame());
    } else {
      log.warn("websocket client: {} not alived when pong.", clientName());
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
        group.shutdownGracefully().addListener(event ->
            log.info("websocket client: {} event group: {} has been shutting down, cause by reconnect.", clientName(), group));
      }
      group = new NioEventLoopGroup(2, ThreadFactoryUtil.getNamedThreadFactory(clientName()));
      log.info("websocket client: {} new event group: {} has been startup.", clientName(), group);
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
        group.shutdownGracefully().addListener(event ->
            log.info("websocket client: {} event group: {} has been shutting down, cause by start error.", clientName(), group));
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

  private void startTopicMessageMonitors(Collection<String> topics) {
    for (String topic : topics) {
      TopicMonitorTask topicMonitorTask = topicMonitorTaskMap.computeIfAbsent(topic, var -> new TopicMonitorTask(this, topic));
      monitorScheduler.scheduleWithFixedDelay(topicMonitorTask, 1000 * 10, 1000, TimeUnit.MILLISECONDS);
      topicMonitorTask.start();
    }
  }

  private void stopTopicMessageMonitors(Collection<String> topics) {
    for (String topic : topics) {
      TopicMonitorTask topicMonitorTask = topicMonitorTaskMap.get(topic);
      if (topicMonitorTask != null) {
        topicMonitorTask.stop();
      }
    }
  }

  private ScheduledExecutorService buildMonitorScheduler() {
    ThreadFactory scheduleThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        getMonitorScheduleExecutorGroupName());
    return new ScheduledThreadPoolExecutor(2, scheduleThreadFactory, new AbortPolicy() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        log.warn("client: {} monitor scheduler {} reject task: {}.", clientName(), e, r);
        super.rejectedExecution(r, e);
      }
    });
  }

  private ScheduledExecutorService buildSubscribeScheduler() {
    ThreadFactory scheduleThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        getSubscribeScheduleExecutorGroupName());
    return new ScheduledThreadPoolExecutor(2, scheduleThreadFactory, new AbortPolicy() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        log.warn("client: {} subscribe scheduler {} reject task: {}.", clientName(), e, r);
        super.rejectedExecution(r, e);
      }
    });
  }


  private String getMonitorScheduleExecutorGroupName() {
    return String.join(CLIENT_NAME_SEP, MONITOR_SCHEDULE_EXECUTOR_GROUP_PREFIX, clientName());
  }

  private String getSubscribeScheduleExecutorGroupName() {
    return String.join(CLIENT_NAME_SEP, SUBSCRIBE_SCHEDULE_EXECUTOR_GROUP_PREFIX, clientName());
  }

  private void heartbeatTopic(String message) {
    String topic = null;
    for (Function<String, String> topicExtractor : messageTopicExtractors) {
      String extractTopic = topicExtractor.apply(message);
      if (StringUtils.isNotBlank(extractTopic)) {
        topic = extractTopic;
        break;
      }
    }
    if (StringUtils.isNotBlank(topic)) {
      TopicMonitorTask topicMonitorTask = topicMonitorTaskMap.get(topic);
      if (topicMonitorTask != null) {
        topicMonitorTask.heartbeat();
      }
    }
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
