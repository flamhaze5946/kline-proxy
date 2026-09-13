package com.zx.quant.klineproxy.client.ws.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import com.zx.quant.klineproxy.client.ws.enums.ProtocolEnum;
import com.zx.quant.klineproxy.client.ws.handler.WebSocketChannelInboundHandler;
import com.zx.quant.klineproxy.client.ws.task.ClientMonitorTask;
import com.zx.quant.klineproxy.client.ws.task.FrameSendTask;
import com.zx.quant.klineproxy.client.ws.task.PingTask;
import com.zx.quant.klineproxy.client.ws.task.TopicsSubscribeTask;
import com.zx.quant.klineproxy.client.ws.task.TopicsUnsubscribeTask;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.ListTopicsEvent;
import com.zx.quant.klineproxy.model.ParsedWebSocketMessage;
import com.zx.quant.klineproxy.model.WebSocketFrameWrapper;
import com.zx.quant.klineproxy.model.WebSocketMessageTiming;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.ExceptionSafeRunnable;
import com.zx.quant.klineproxy.util.Serializer;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import com.zx.quant.klineproxy.util.queue.HashSetQueue;
import com.zx.quant.klineproxy.util.queue.SetQueue;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelOption;
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
import io.netty.handler.codec.http.websocketx.WebSocketFrameAggregator;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketServerCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.stream.ChunkedWriteHandler;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
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
import java.util.concurrent.RejectedExecutionException;
import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import com.zx.quant.klineproxy.client.ws.dispatch.KlineMessageDispatcher;
import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
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

  private static final int MAX_WEBSOCKET_FRAME_SIZE = 10 * 1024 * 1024;

  private static final String CLIENT_NAME_SEP = "-";

  private static final String COMMON_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-common";

  private static final String MONITOR_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-monitor";

  private static final String SUBSCRIBE_SCHEDULE_EXECUTOR_GROUP_PREFIX = "websocket-subscribe";

  private static final String MESSAGE_EXECUTOR_GROUP_PREFIX = "websocket-control-handler";

  private static final long TOPIC_MONITOR_INTERVAL_MILLS = 30_000L;

  private static final long TOPIC_MESSAGE_TIMEOUT_MILLS = 120_000L;

  private static final AtomicLong MESSAGE_RECEIVE_SEQUENCE = new AtomicLong(0);
  private static final AtomicLong GENERIC_TASKS = new AtomicLong(0);

  private static final ThreadPoolExecutor MESSAGE_EXECUTOR = buildMessageExecutor();
  private static final KlineMessageDispatcher FALLBACK_KLINE_DISPATCHER =
      new KlineMessageDispatcher(new KlineIngressProperties());

  @Autowired(required = false)
  private KlineMessageDispatcher klineMessageDispatcher;
  private volatile Function<String, KlineDispatchMetadata> klineMessageClassifier = raw -> null;

  private static final String PING = "PING";

  private static final String PONG = "PONG";

  private final AtomicBoolean launching;
  private final AtomicBoolean closed = new AtomicBoolean();

  private final Supplier<WebSocketChannelInboundHandler> handlerSupplier;

  private final List<Function<ParsedWebSocketMessage, Boolean>> messageHandlers;

  private final List<Function<ParsedWebSocketMessage, String>> messageTopicExtractors;

  private final SetQueue<String> candidateSubscribeTopics;

  private final SetQueue<String> candidateUnsubscribeTopics;

  private final Queue<WebSocketFrameWrapper> candidateFrameWrappers;

  private final Map<String, Long> topicLastMessageTimeMap;

  protected WebSocketChannelInboundHandler inboundHandler;

  protected final Set<String> registeredTopics;

  protected final Set<String> channelRegisteredTopics;

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
    this.channelRegisteredTopics = new ConcurrentSkipListSet<>();
    this.messageHandlers = new ArrayList<>();
    this.messageTopicExtractors = new ArrayList<>();
    this.candidateSubscribeTopics = new HashSetQueue<>();
    this.candidateUnsubscribeTopics = new HashSetQueue<>();
    this.candidateFrameWrappers = new LinkedBlockingQueue<>();
    this.topicLastMessageTimeMap = new ConcurrentHashMap<>();
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
        registerTopicHeartbeats(topics);
      }
      log.info("websocket client: {} subscribe topics: {} message sent.", clientName(), topics);
    };
    sendData(frame, afterSendFunc);
  }

  private void executeListTopicsTask() {
    if (!alive()) {
      log.warn("websocket client: {} not alive when unsubscribe topics", clientName());
      throw new RuntimeException("websocket client not alive when unsubscribe topics");
    }
    WebSocketFrame frame = buildListTopicsFrame();
    sendData(frame, null);
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
        unregisterTopicHeartbeats(topics);
      }
      log.info("websocket client: {} unsubscribe topics: {} message sent.", clientName(), topics);
    };
    sendData(frame, afterSendFunc);
  }

  public void start() {
    synchronized (launching) {
      if (closed.get()) {
        return;
      }
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

      releaseQueuedFrames();
      this.candidateSubscribeTopics.clear();
      this.candidateUnsubscribeTopics.clear();
      this.topicLastMessageTimeMap.clear();
      this.registeredTopics.clear();
      this.channelRegisteredTopics.clear();

      this.connect();

      this.commonScheduler = buildCommonExecutor();

      this.clientMonitorTask = new ClientMonitorTask(this);
      PingTask pingTask = new PingTask(this);
      monitorScheduler = buildMonitorScheduler();
      monitorScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(clientMonitorTask), 1000 * 5, 1000, TimeUnit.MILLISECONDS);
      monitorScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(pingTask), 1000 * 30, 1000 * 60, TimeUnit.MILLISECONDS);
      monitorScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(this::executeListTopicsTask), 1000 * 10, 1000 * 30, TimeUnit.MILLISECONDS);
      monitorScheduler.scheduleWithFixedDelay(
          new SwitchableRunnable(this::monitorTopics), 1000 * 10, TOPIC_MONITOR_INTERVAL_MILLS, TimeUnit.MILLISECONDS);

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
  public synchronized void addMessageHandler(Function<ParsedWebSocketMessage, Boolean> messageHandler) {
    this.messageHandlers.add(messageHandler);
  }

  @Override
  public void addMessageTopicExtractorHandler(Function<ParsedWebSocketMessage, String> messageTopicExtractor) {
    this.messageTopicExtractors.add(messageTopicExtractor);
  }

  @Override
  public void onReceive(String message) {
    long receivedAtNanos = System.nanoTime();
    onReceive(message, System.currentTimeMillis(), receivedAtNanos);
  }

  @Override
  public void onReceive(String message, long receivedAtMillis, long receivedAtNanos) {
    heartbeatTransport();
    long receiveSequence = MESSAGE_RECEIVE_SEQUENCE.incrementAndGet();
    KlineDispatchMetadata metadata;
    try {
      metadata = klineMessageClassifier.apply(message);
    } catch (RuntimeException error) {
      log.warn("websocket client: {} ingress classification failed; using reliable generic handler.", clientName(), error);
      metadata = null;
    }
    if (metadata != null) {
      KlineMessageDispatcher dispatcher = klineDispatcher();
      KlineDispatchMetadata classified = metadata;
      // Detailed latency is a closing-bar diagnostic. Avoid timing objects, clock reads and queue
      // snapshots for the high-volume forming updates that may be coalesced before execution.
      WebSocketMessageTiming timing = metadata.closed()
          ? new WebSocketMessageTiming(clientName(), receivedAtMillis, receivedAtNanos) : null;
      topicLastMessageTimeMap.put(metadata.topic(), receivedAtMillis);
      if (timing != null) {
        timing.enqueued(dispatcher.queuedTasks());
      }
      dispatcher.submit(metadata, receiveSequence, () -> {
        if (timing != null) {
          timing.handlerStarted(dispatcher.queuedTasks());
        }
        handleMessage(message, timing, receiveSequence, dispatcher, classified);
      });
      return;
    }
    WebSocketMessageTiming timing = new WebSocketMessageTiming(clientName(), receivedAtMillis, receivedAtNanos);
    timing.enqueued(MESSAGE_EXECUTOR.getQueue().size());
    GENERIC_TASKS.incrementAndGet();
    try {
      MESSAGE_EXECUTOR.execute(() -> {
        try {
          timing.handlerStarted(MESSAGE_EXECUTOR.getQueue().size());
          handleMessage(message, timing, receiveSequence);
        } finally {
          GENERIC_TASKS.decrementAndGet();
        }
      });
    } catch (RuntimeException | Error error) {
      GENERIC_TASKS.decrementAndGet();
      throw error;
    }
  }

  @Override
  public void setKlineMessageClassifier(Function<String, KlineDispatchMetadata> classifier) {
    klineMessageClassifier = java.util.Objects.requireNonNull(classifier);
  }

  private KlineMessageDispatcher klineDispatcher() {
    return klineMessageDispatcher != null ? klineMessageDispatcher : FALLBACK_KLINE_DISPATCHER;
  }

  @Override
  public void onReceiveNoHandle() {
    heartbeatTransport();
  }

  private void heartbeatTransport() {
    ClientMonitorTask monitor = clientMonitorTask;
    if (monitor != null) {
      monitor.heartbeat();
    }
  }

  /** Call only after every producer's close has finished its in-flight channel callbacks. */
  public static boolean awaitGenericMessageTasks(Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (GENERIC_TASKS.get() != 0) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0 || Thread.currentThread().isInterrupted()) {
        return false;
      }
      LockSupport.parkNanos(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(1)));
    }
    return true;
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

  protected abstract WebSocketFrame buildListTopicsFrame();

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
  public List<String> getChannelRegisteredTopics() {
    return List.copyOf(channelRegisteredTopics);
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
      if (closed.get()) {
        return;
      }
      try {
        subId = generateSubId();
        inboundHandler = handlerSupplier.get();
        inboundHandler.init(this);

        connectWebSocket();
        if (alive()) {
          if (!inboundHandler.getHandshakeFuture().await(10, TimeUnit.SECONDS)
              || !inboundHandler.getHandshakeFuture().isSuccess()) {
            throw new IllegalStateException("WebSocket handshake failed or timed out");
          }
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
      if (closed.get()) {
        return;
      }
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
    return !closed.get() && channel != null && channel.isActive();
  }

  @Override
  public void close() {
    ChannelFuture closeFuture = null;
    synchronized (launching) {
      closed.set(true);
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
      if (channel != null) {
        closeFuture = channel.close();
      }
      releaseQueuedFrames();
    }
    // Do not hold launching while the event loop completes callbacks/reconnect attempts.
    if (closeFuture != null && !closeFuture.channel().eventLoop().inEventLoop()
        && !closeFuture.awaitUninterruptibly(10, TimeUnit.SECONDS)) {
      log.error("WebSocket producer {} did not stop its channel within 10 seconds", clientName());
    }
    shutdownGroup("client closed");
  }

  private void releaseQueuedFrames() {
    synchronized (candidateFrameWrappers) {
      WebSocketFrameWrapper wrapper;
      while ((wrapper = candidateFrameWrappers.poll()) != null) {
        wrapper.frame().release();
      }
    }
  }

  @Override
  public void sendData(WebSocketFrame frame, Runnable afterSendFunc) {
    if (afterSendFunc == null) {
      // Capture text before Netty takes ownership; an async callback must not read a released buf.
      String frameLog = frame instanceof TextWebSocketFrame text ? text.text() : frame.getClass().getSimpleName();
      afterSendFunc = () -> log.info("client: {} data: {} sent.", clientName(), frameLog);
    }
    synchronized (candidateFrameWrappers) {
      if (!alive()) {
        log.warn("client: {} not alive, frame send failed.", clientName());
        frame.release();
        return;
      }
      candidateFrameWrappers.offer(new WebSocketFrameWrapper(frame, afterSendFunc));
    }
  }

  public void sendData0(WebSocketFrame frame) {
    if (!alive()) {
      log.warn("client: {} not alive, data: {} send failed.", clientName(), frame);
      frame.release();
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

  protected void connectWebSocket() {
    try {
      URI realUri = uri();
      String protocol = realUri.getScheme();
      ProtocolEnum protocolEnum = CommonUtil.getEnumByCode(protocol, ProtocolEnum.class);
      if (protocolEnum == null) {
        throw new RuntimeException("protocol not supported.");
      }

      String host = realUri.getHost();
      int port = protocolEnum.getPort();
      boolean ssl = protocolEnum.isSsl();
      final SslContext sslCtx;
      if (ssl) {
        sslCtx = SslContextBuilder
            .forClient()
            .trustManager(InsecureTrustManagerFactory.INSTANCE)
            .build();
      } else {
        sslCtx = null;
      }
      shutdownGroup("reconnect");
      group = new NioEventLoopGroup(1, ThreadFactoryUtil.getNamedThreadFactory(clientName()));
      log.info("websocket client: {} new event group: {} has been startup.", clientName(), group);
      Bootstrap bootstrap = new Bootstrap();
      bootstrap
          .group(group)
          .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10_000)
          .channel(NioSocketChannel.class)
          .handler(
              new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                  ChannelPipeline pipeline = ch.pipeline();
                  if (sslCtx != null) {
                    pipeline.addLast(sslCtx.newHandler(ch.alloc(), host, port));
                  }
                  pipeline.addLast(
                      new HttpClientCodec(),
                      new ChunkedWriteHandler(),
                      new HttpObjectAggregator(8192),
                      new WebSocketServerCompressionHandler(),
                      new WebSocketFrameAggregator(MAX_WEBSOCKET_FRAME_SIZE),
                      inboundHandler);
                }
              });
      channel = bootstrap.connect(host, port)
          .addListener(f -> {
            ChannelFuture cf = (ChannelFuture) f;
            if (!cf.isSuccess()) {
              long nextRetryDelay = 1000L;
              cf.channel().eventLoop().schedule(this::connect, nextRetryDelay, TimeUnit.MILLISECONDS);
            }
          })
          .sync().channel();
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

  private void registerTopicHeartbeats(Collection<String> topics) {
    long now = System.currentTimeMillis();
    for (String topic : topics) {
      topicLastMessageTimeMap.put(topic, now);
    }
  }

  private void unregisterTopicHeartbeats(Collection<String> topics) {
    for (String topic : topics) {
      topicLastMessageTimeMap.remove(topic);
    }
  }

  private void monitorTopics() {
    if (!monitorTopicMessage() || topicLastMessageTimeMap.isEmpty()) {
      return;
    }
    long now = System.currentTimeMillis();
    topicLastMessageTimeMap.forEach((topic, lastMessageTime) -> {
      if (lastMessageTime == null || now - lastMessageTime <= TOPIC_MESSAGE_TIMEOUT_MILLS) {
        return;
      }
      if (channelRegisteredTopics.contains(topic)) {
        topicLastMessageTimeMap.put(topic, now);
        return;
      }
      log.info("{}ms not received messages from client {} for topic: {}, resubscribe.",
          TOPIC_MESSAGE_TIMEOUT_MILLS, clientName(), topic);
      topicLastMessageTimeMap.put(topic, now);
      subscribeTopic(topic);
    });
  }

  private ExecutorService buildCommonExecutor() {
    ThreadFactory threadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        getCommonScheduleExecutorGroupName());
    return new ThreadPoolExecutor(
        2, 10,
        1, TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(1024), threadFactory, new CallerRunsPolicy());
  }

  private ScheduledExecutorService buildMonitorScheduler() {
    ThreadFactory scheduleThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        getMonitorScheduleExecutorGroupName());
    return new ScheduledThreadPoolExecutor(1, scheduleThreadFactory, new AbortPolicy() {
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
    return new ScheduledThreadPoolExecutor(1, scheduleThreadFactory, new AbortPolicy() {
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

  private void heartbeatTopic(ParsedWebSocketMessage parsedMessage) {
    String topic = extractTopicFromMessage(parsedMessage);
    if (StringUtils.isNotBlank(topic)) {
      long now = System.currentTimeMillis();
      topicLastMessageTimeMap.computeIfPresent(topic, (ignore, lastMessageTime) -> now);
    }
  }

  private String extractTopicFromMessage(ParsedWebSocketMessage parsedMessage) {
    if (parsedMessage.combined()) {
      return parsedMessage.stream();
    }

    for (Function<ParsedWebSocketMessage, String> topicExtractor : messageTopicExtractors) {
      String extractTopic = topicExtractor.apply(parsedMessage);
      if (StringUtils.isNotBlank(extractTopic)) {
        return extractTopic;
      }
    }
    return null;
  }

  private void handleMessage(String message, WebSocketMessageTiming timing, long receiveSequence) {
    handleMessage(message, timing, receiveSequence, null, null);
  }

  private void handleMessage(String message, WebSocketMessageTiming timing, long receiveSequence,
      KlineMessageDispatcher dispatcher, KlineDispatchMetadata metadata) {
    try {
      if (StringUtils.equals(StringUtils.trim(message), PING)) {
        this.sendMessage(PONG);
        return;
      }
      if (StringUtils.equals(StringUtils.trim(message), PONG)) {
        return;
      }

      ParsedWebSocketMessage parsedMessage = parseMessage(message, timing, receiveSequence, metadata);
      if (timing != null) {
        timing.jsonParsed();
      }
      if (timing != null && parsedMessage.payloadObject()
          && parsedMessage.payloadNode().path("k").path("x").asBoolean(false)) {
        timing.executorSnapshot(dispatcher != null ? dispatcher.activeWorkers() : MESSAGE_EXECUTOR.getActiveCount(),
            dispatcher != null ? dispatcher.workerCount() : MESSAGE_EXECUTOR.getPoolSize(), 0L);
      }
      if (metadata == null) {
        heartbeatTopic(parsedMessage);
      } // classified frames already recorded their heartbeat at receipt, before any queue delay
      if (isListTopicsMessage(parsedMessage.rootNode())) {
        ListTopicsEvent listTopicsEvent = serializer.treeToValue(parsedMessage.rootNode(), ListTopicsEvent.class);
        synchronized (channelRegisteredTopics) {
          channelRegisteredTopics.clear();
          channelRegisteredTopics.addAll(listTopicsEvent.getResult());
        }
        return;
      }

      for (Function<ParsedWebSocketMessage, Boolean> messageHandler : messageHandlers) {
        boolean handled = messageHandler.apply(parsedMessage);
        if (handled) {
          return;
        }
      }

      if (!isWebSocketResponseMessage(parsedMessage)) {
        if (dispatcher != null) {
          throw new IllegalStateException("Classified kline was not handled: " + metadata.bar());
        }
        log.info("not handlable message received: {}", parsedMessage.rawMessage());
      }
    } catch (RuntimeException e) {
      if (dispatcher != null) {
        throw e; // dispatcher reports the failed attempt separately from processed counters
      }
      log.warn("websocket client: {} handle message failed.", clientName(), e);
    }
  }

  private ParsedWebSocketMessage parseMessage(String message, WebSocketMessageTiming timing, long receiveSequence,
      KlineDispatchMetadata metadata) {
    JsonNode rootNode = serializer.readTree(message);
    JsonNode payloadNode = rootNode;
    String stream = extractTextField(rootNode, "stream");
    JsonNode dataNode = rootNode.get("data");
    if (StringUtils.isNotBlank(stream) && dataNode != null && !dataNode.isNull()) {
      payloadNode = dataNode;
    }
    return new ParsedWebSocketMessage(message, rootNode, payloadNode, stream, extractEventType(payloadNode), timing,
        receiveSequence, metadata);
  }

  private String extractEventType(JsonNode payloadNode) {
    if (payloadNode == null || payloadNode.isNull()) {
      return null;
    }
    if (payloadNode.isArray()) {
      if (payloadNode.isEmpty()) {
        return null;
      }
      return extractTextField(payloadNode.get(0), "e");
    }
    return extractTextField(payloadNode, "e");
  }

  private String extractTextField(JsonNode jsonNode, String fieldName) {
    if (jsonNode == null || !jsonNode.isObject()) {
      return null;
    }
    JsonNode fieldNode = jsonNode.get(fieldName);
    if (fieldNode == null || fieldNode.isNull()) {
      return null;
    }
    return fieldNode.asText();
  }

  private boolean isWebSocketResponseMessage(ParsedWebSocketMessage parsedMessage) {
    JsonNode rootNode = parsedMessage.rootNode();
    return rootNode != null && rootNode.isObject() && rootNode.has("result") && rootNode.has("id");
  }

  private boolean isListTopicsMessage(JsonNode rootNode) {
    return rootNode != null
        && rootNode.isObject()
        && rootNode.has("id")
        && rootNode.has("result")
        && rootNode.get("result").isArray();
  }

  private static ThreadPoolExecutor buildMessageExecutor() {
    ThreadFactory namedThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        MESSAGE_EXECUTOR_GROUP_PREFIX);
    return new ThreadPoolExecutor(
        2,
        2,
        1,
        TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(4096),
        task -> {
          Thread thread = namedThreadFactory.newThread(task);
          thread.setDaemon(true); // the Spring lifecycle explicitly drains work before destruction
          return thread;
        },
        (task, executor) -> {
          boolean interrupted = false;
          try {
            while (!executor.isShutdown()) {
              try {
                if (executor.getQueue().offer(task, 100, TimeUnit.MILLISECONDS)) {
                  return;
                }
              } catch (InterruptedException ignored) {
                interrupted = true;
              }
            }
            throw new RejectedExecutionException("WebSocket generic handler is shutting down");
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        });
  }

  private class SwitchableRunnable implements Runnable {

    private final Runnable target;

    private SwitchableRunnable(Runnable target) {
      this.target = new ExceptionSafeRunnable(target);
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
