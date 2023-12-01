package com.zx.quant.klineproxy.client.ws.client;

import com.google.common.collect.Sets;
import com.zx.quant.klineproxy.client.ws.enums.ProtocolEnum;
import com.zx.quant.klineproxy.client.ws.handler.WebSocketChannelInboundHandler;
import com.zx.quant.klineproxy.client.ws.task.ClientMonitorTask;
import com.zx.quant.klineproxy.client.ws.task.FrameSendTask;
import com.zx.quant.klineproxy.client.ws.task.PingTask;
import com.zx.quant.klineproxy.client.ws.task.TopicMonitorTask;
import com.zx.quant.klineproxy.client.ws.task.TopicsSubscribeTask;
import com.zx.quant.klineproxy.client.ws.task.TopicsUnsubscribeTask;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.WebSocketFrameWrapper;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import com.zx.quant.klineproxy.util.queue.HashSetQueue;
import com.zx.quant.klineproxy.util.queue.SetQueue;
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
import java.util.concurrent.atomic.AtomicBoolean;
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

  private static final String COMMON_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-common";

  private static final String MONITOR_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-monitor";

  private static final String SUBSCRIBE_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-subscribe";

  private static final String MESSAGE_EXECUTOR_GROUP_PREFIX = "websocket-message-handler";

  private static final ExecutorService MESSAGE_EXECUTOR = buildMessageExecutor();

  private static final String PING = "PING";

  private static final String PONG = "PONG";

  private final AtomicBoolean launching;

  private final Supplier<WebSocketChannelInboundHandler> handlerSupplier;

  private final List<Function<String, Boolean>> messageHandlers;

  private final List<Function<String, String>> messageTopicExtractors;

  private final SetQueue<String> candidateSubscribeTopics;

  private final SetQueue<String> candidateUnsubscribeTopics;

  private final Queue<WebSocketFrameWrapper> candidateFrameWrappers;

  private final Map<String, TopicMonitorTask> topicMonitorTaskMap;

  protected final Set<String> registeredTopics;

  protected final int clientNumber;

  @Autowired
  protected Serializer serializer;

  @Autowired
  protected RateLimitManager rateLimitManager;

  protected T subId;

  protected volatile URI uri;

  protected Channel channel;

  protected EventLoopGroup group;

  protected ClientMonitorTask clientMonitorTask;

  private ExecutorService commonScheduler;

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
    this.launching = new AtomicBoolean(false);
    this.clientNumber = clientNumber;
    this.handlerSupplier = handlerSupplier;
    this.registeredTopics = new ConcurrentSkipListSet<>();
    this.messageHandlers = new ArrayList<>();
    this.messageTopicExtractors = new ArrayList<>();
    this.candidateSubscribeTopics = new HashSetQueue<>();
    this.candidateUnsubscribeTopics = new HashSetQueue<>();
    this.candidateFrameWrappers = new LinkedBlockingQueue<>();
    this.topicMonitorTaskMap = new ConcurrentHashMap<>();
  }

  private void executeSubscribeTask(Collection<String> topics) {
    if (!alive()) {
      log.warn("websocket client: {} not alive when subscribe topics", clientName());
      throw new RuntimeException("websocket client not alive when subscribe topics");
    }
    WebSocketFrame frame = buildSubscribeFrame(topics);
    Runnable afterSendFunc = () -> {
      registeredTopics.addAll(topics);
      if (monitorTopicMessage()) {
        startTopicMessageMonitors(topics);
      }
      log.info("websocket client: {} subscribe topics: {} message sent.", clientName(), topics);
    };
    sendData(frame, afterSendFunc);
  }

  private void executeUnsubscribeTask(Collection<String> topics) {
    if (!alive()) {
      log.warn("websocket client: {} not alive when unsubscribe topics", clientName());
      throw new RuntimeException("websocket client not alive when unsubscribe topics");
    }
    WebSocketFrame frame = buildUnsubscribeFrame(topics);
    Runnable afterSendFunc = () -> {
      registeredTopics.removeAll(Sets.newHashSet(topics));
      if (monitorTopicMessage()) {
        stopTopicMessageMonitors(topics);
      }
      log.info("websocket client: {} unsubscribe topics: {} message sent.", clientName(), topics);
    };
    sendData(frame, afterSendFunc);
  }

  public void start() {
    synchronized (launching) {
      launching.set(false);
      if (monitorScheduler != null) {
        monitorScheduler.shutdown();
      }
      if (subscribeScheduler != null) {
        subscribeScheduler.shutdown();
      }
      if (commonScheduler != null) {
        commonScheduler.shutdown();
      }

      this.candidateFrameWrappers.clear();
      this.candidateSubscribeTopics.clear();
      this.candidateUnsubscribeTopics.clear();
      this.topicMonitorTaskMap.clear();
      this.registeredTopics.clear();

      this.connect();

      this.commonScheduler = buildCommonExecutor();

      this.clientMonitorTask = new ClientMonitorTask(this);
      PingTask pingTask = new PingTask(this);
      monitorScheduler = buildMonitorScheduler();
      monitorScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(clientMonitorTask), 1000 * 5, 1000, TimeUnit.MILLISECONDS);
      monitorScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(pingTask), 1000 * 30, 1000 * 60, TimeUnit.MILLISECONDS);

      FrameSendTask frameSendTask = new FrameSendTask(this::sendData0, this.candidateFrameWrappers, commonScheduler);
      long millsIntervalForFrameSend = 1000 / getMaxFramesPerSecond();
      TopicsSubscribeTask subscribeTask = new TopicsSubscribeTask(this.candidateSubscribeTopics, getMaxTopicsPerTime(), this::executeSubscribeTask);
      TopicsUnsubscribeTask unsubscribeTask = new TopicsUnsubscribeTask(this.candidateUnsubscribeTopics, getMaxTopicsPerTime(), this::executeUnsubscribeTask);
      subscribeScheduler = buildSubscribeScheduler();
      subscribeScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(frameSendTask), 0, millsIntervalForFrameSend, TimeUnit.MILLISECONDS);
      subscribeScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(subscribeTask), 0, 5000, TimeUnit.MILLISECONDS);
      subscribeScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(unsubscribeTask), 0, 5000, TimeUnit.MILLISECONDS);

      launching.set(true);
    }
  }

  @Override
  public synchronized void addMessageHandler(Function<String, Boolean> messageHandler) {
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

      for (Function<String, Boolean> messageHandler : messageHandlers) {
        boolean handled = messageHandler.apply(message);
        if (handled) {
          return;
        }
      }
    }, MESSAGE_EXECUTOR);
  }

  @Override
  public void onReceiveNoHandle() {
    clientMonitorTask.heartbeat();
  }

  @Override
  public synchronized void subscribeTopics(Collection<String> topics) {
    candidateSubscribeTopics.offerAll(topics);
  }

  @Override
  public synchronized void unsubscribeTopics(Collection<String> topics) {
    candidateUnsubscribeTopics.offerAll(topics);
  }

  protected abstract WebSocketFrame buildSubscribeFrame(Collection<String> topics);

  protected abstract WebSocketFrame buildUnsubscribeFrame(Collection<String> topics);

  protected abstract int getMaxTopicsPerTime();

  protected abstract int getMaxFramesPerSecond();

  protected abstract boolean monitorTopicMessage();

  protected String globalFrameSendRateLimiter() {
    return null;
  };

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
    synchronized (launching) {
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
        shutdownGroup("connect error");
      }
    }
  }

  @Override
  public void reconnect() {
    synchronized (launching) {
      launching.set(false);
      try{
        log.info("websocket client: {} start to reconnect.", clientName());
        this.connect();
      } catch (Exception e) {
        log.warn("websocket client: {} reconnect failed.", clientName(), e);
      } finally{
        log.warn("websocket client: {} reconnect complete.", clientName());
      }
      launching.set(true);
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
  public void sendData(WebSocketFrame frame, Runnable afterSendFunc) {
    if (!alive()) {
      log.warn("client: {} not alive, data: {} send failed.", clientName(), frame);
      return;
    }

    if (afterSendFunc == null) {
      afterSendFunc = () -> {
        String frameLog;
        if (frame instanceof TextWebSocketFrame textWebSocketFrame) {
          frameLog = textWebSocketFrame.text();
        } else {
          frameLog = frame.getClass().getSimpleName();
        }
        log.info("client: {} data: {} sent.", clientName(), frameLog);
      };
    }
    WebSocketFrameWrapper wrapper = new WebSocketFrameWrapper(frame, afterSendFunc);
    candidateFrameWrappers.offer(wrapper);
  }

  public void sendData0(WebSocketFrame frame) {
    if (!alive()) {
      log.warn("client: {} not alive, data: {} send failed.", clientName(), frame);
      return;
    }
    String limiterName = globalFrameSendRateLimiter();
    if (limiterName != null) {
      rateLimitManager.acquire(limiterName, 1);
    }

    channel.writeAndFlush(frame);
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
      shutdownGroup("reconnect");
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
              pipeline.addLast(new HttpClientCodec(), new HttpObjectAggregator(8192), handler);
            }
          });
      channel = bootstrap.connect(host, port).sync().channel();
    } catch (Exception e) {
      log.error(" websocket client: {} start error.", clientName(), e);
      shutdownGroup("start error");
    }
  }

  protected URI buildUri() {
    try {
      return new URI(getUrl());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private void shutdownGroup(String causeBy) {
    if (group != null && !group.isShutdown() && !group.isShuttingDown()) {
      group.shutdownGracefully().addListener(event ->
          log.info("websocket client: {} event group: {} has been shutting down, cause by {}.", clientName(), group, causeBy));
    }
  }

  private void startTopicMessageMonitors(Collection<String> topics) {
    synchronized (topicMonitorTaskMap) {
      for (String topic : topics) {
        TopicMonitorTask topicMonitorTask = topicMonitorTaskMap.get(topic);
        if (topicMonitorTask == null) {
          topicMonitorTask = new TopicMonitorTask(this, topic);
          topicMonitorTaskMap.put(topic, topicMonitorTask);
          monitorScheduler.scheduleWithFixedDelay(
              new SwitchableRunnable(topicMonitorTask), 1000 * 10, 1000, TimeUnit.MILLISECONDS);
        }
        topicMonitorTask.start();
      }
    }
  }

  private void stopTopicMessageMonitors(Collection<String> topics) {
    synchronized (topicMonitorTaskMap) {
      for (String topic : topics) {
        TopicMonitorTask topicMonitorTask = topicMonitorTaskMap.get(topic);
        if (topicMonitorTask != null) {
          topicMonitorTask.stop();
        }
      }
    }
  }

  private ExecutorService buildCommonExecutor() {
    ThreadFactory threadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        getMonitorScheduleExecutorGroupName());
    return new ThreadPoolExecutor(
        2, 10,
        1, TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(1024), threadFactory, new CallerRunsPolicy());
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

  private String getCommonScheduleExecutorGroupName() {
    return String.join(CLIENT_NAME_SEP, COMMON_SCHEDULE_EXECUTOR_GROUP_PREFIX, clientName());
  }

  private String getMonitorScheduleExecutorGroupName() {
    return String.join(CLIENT_NAME_SEP, MONITOR_SCHEDULE_EXECUTOR_GROUP_PREFIX, clientName());
  }

  private String getSubscribeScheduleExecutorGroupName() {
    return String.join(CLIENT_NAME_SEP, SUBSCRIBE_SCHEDULE_EXECUTOR_GROUP_PREFIX, clientName());
  }

  private void heartbeatTopic(String message) {
    String topic = extractTopicFromMessage(message);
    if (StringUtils.isNotBlank(topic)) {
      TopicMonitorTask topicMonitorTask = topicMonitorTaskMap.get(topic);
      if (topicMonitorTask != null) {
        topicMonitorTask.heartbeat();
      }
    }
  }

  private String extractTopicFromMessage(String message) {
    for (Function<String, String> topicExtractor : messageTopicExtractors) {
      String extractTopic = topicExtractor.apply(message);
      if (StringUtils.isNotBlank(extractTopic)) {
        return extractTopic;
      }
    }
    return null;
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

  private class SwitchableRunnable implements Runnable {

    private final Runnable target;

    private SwitchableRunnable(Runnable target) {
      this.target = target;
    }

    @Override
    public void run() {
      if (!launching.get()) {
        return;
      }
      target.run();
    }
  }
}
