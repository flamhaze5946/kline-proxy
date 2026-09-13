package com.zx.quant.klineproxy.service.impl;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.model.*;
import com.zx.quant.klineproxy.client.ws.client.*;
import com.zx.quant.klineproxy.client.ws.task.ClientMonitorTask;
import com.zx.quant.klineproxy.model.*;
import com.zx.quant.klineproxy.model.config.*;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.*;
import com.zx.quant.klineproxy.monitor.impl.MonitorManagerImpl;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.util.Serializer;
import io.netty.handler.codec.http.websocketx.*;
import io.prometheus.client.CollectorRegistry;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import jdk.jfr.*;
import org.slf4j.LoggerFactory;

/** Research harness. Calls production websocket parsing, protocols, cache and bulk services.
 * Synthetic prices/timing, production-size retained history. No application startup or network.
 */
public class ReplayHarness {
  static final long H = 3_600_000L, D = 24*H, BASE = 1_789_308_000_000L;
  static final ObjectMapper JSON = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
  static final Serializer SERIALIZER = new Serializer(JSON);
  static final com.sun.management.OperatingSystemMXBean OS =
      (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  static final ThreadPoolExecutor FIFO = (ThreadPoolExecutor) field(AbstractWebSocketClient.class, null, "MESSAGE_EXECUTOR");
  static final AtomicLong DROPS = optionalField(AbstractWebSocketClient.class, "MESSAGE_TASK_DROP_COUNT")
      instanceof AtomicLong counter ? counter : new AtomicLong();
  static final Object PRODUCTION_DISPATCHER = optionalField(AbstractWebSocketClient.class, "FALLBACK_KLINE_DISPATCHER");
  static Object optionalField(Class<?> type, String name) {
    try {return field(type,null,name);} catch(IllegalArgumentException missing) {return null;}
  }
  static Map<String,Long> dispatcherStats() {
    if(PRODUCTION_DISPATCHER==null)return Map.of();
    try {
      Object snapshot=PRODUCTION_DISPATCHER.getClass().getMethod("snapshot").invoke(PRODUCTION_DISPATCHER);
      var result=new LinkedHashMap<String,Long>();
      for(var part:snapshot.getClass().getRecordComponents()) result.put(part.getName(),((Number)part.getAccessor().invoke(snapshot)).longValue());
      return result;
    }catch(Exception e){throw new RuntimeException(e);}
  }
  static boolean productionIdle() {
    if(PRODUCTION_DISPATCHER==null)return true;
    try{return (boolean)PRODUCTION_DISPATCHER.getClass().getMethod("isIdle").invoke(PRODUCTION_DISPATCHER);}
    catch(Exception e){throw new RuntimeException(e);}
  }
  static volatile Round current;
  static ThreadPoolExecutor READERS;
  static final ScheduledExecutorService BULK_STARTER=Executors.newSingleThreadScheduledExecutor();
  static volatile long clockBoundary = BASE, clockStart = System.nanoTime();
  static long serverTime() { return clockBoundary - 20 + (System.nanoTime()-clockStart)/1_000_000; }

  static Object field(Class<?> type, Object owner, String name) {
    while(type != null) {
      try { var f=type.getDeclaredField(name);f.setAccessible(true);return f.get(owner); }
      catch(NoSuchFieldException e) {type=type.getSuperclass();}
      catch(Exception e) {throw new RuntimeException(e);}
    }
    throw new IllegalArgumentException(name);
  }
  static void set(Object owner, String name, Object value) {
    Class<?> type=owner.getClass();
    while(type != null) {
      try {var f=type.getDeclaredField(name);f.setAccessible(true);f.set(owner,value);return;}
      catch(NoSuchFieldException e) {type=type.getSuperclass();}
      catch(Exception e) {throw new RuntimeException(e);}
    }
    throw new IllegalArgumentException(name);
  }
  static double ms(long nanos) {return nanos/1e6;}
  static double quantile(Collection<Double> values,double q) {
    if(values.isEmpty()) return -1;
    var v=values.stream().sorted().toList();return v.get(Math.min(v.size()-1,(int)(q*v.size())));
  }

  static class Fixture {
    final boolean future;
    final AbstractKlineService<?> service;
    final List<String> symbols;
    final Client client;
    Fixture(boolean future,int count,int history,boolean diagnostics) {
      this.future=future;
      symbols=new ArrayList<>();
      for(int i=0;i<count;i++) symbols.add((future?"F":"S")+String.format(Locale.ROOT,"%04d",i)+"USDT");
      if(future) {
        var s=new BinanceFutureKlineServiceImpl();
        var exchange=new BinanceFutureExchange();
        var meta=new ArrayList<BinanceFutureSymbol>();
        for(int i=0;i<count;i++) {
          var m=new BinanceFutureSymbol();m.setSymbol(symbols.get(i));m.setPair(symbols.get(i));
          m.setStatus("TRADING");m.setContractType(i<528?"PERPETUAL":"TRADIFI_PERPETUAL");meta.add(m);
        }
        exchange.setSymbols(meta);
        set(s,"exchangeService",new ExchangeService<BinanceFutureExchange>() {
          public BinanceFutureExchange queryExchange(){return exchange;}
          public long queryServerTime(){return serverTime();}
          public List<String> querySymbols(){return meta.stream().filter(x->"TRADING".equals(x.getStatus())).map(BinanceFutureSymbol::getSymbol).toList();}
        });
        var config=new BinanceFutureKlineSyncConfigProperties();
        var h=new IntervalSyncFutureConfig();h.setMinMaintainCount(history);h.setListenSymbolPatterns(List.of(".*"));h.setUseContinuousKlineStream(true);
        var d=new IntervalSyncFutureConfig();d.setMinMaintainCount(history);d.setListenSymbolPatterns(List.of(".*"));
        config.setIntervalSyncConfigs(Map.of("1h",h,"1d",d));set(s,"klineSyncConfigProperties",config);service=s;
      } else {
        var s=new BinanceSpotKlineServiceImpl();
        var exchange=new BinanceSpotExchange();
        var meta=new ArrayList<BinanceSpotSymbol>();
        for(String symbol:symbols){var m=new BinanceSpotSymbol();m.setSymbol(symbol);m.setStatus("TRADING");meta.add(m);}
        exchange.setSymbols(meta);
        set(s,"exchangeService",new ExchangeService<BinanceSpotExchange>() {
          public BinanceSpotExchange queryExchange(){return exchange;}
          public long queryServerTime(){return serverTime();}
          public List<String> querySymbols(){return meta.stream().filter(x->"TRADING".equals(x.getStatus())).map(BinanceSpotSymbol::getSymbol).toList();}
        });
        var config=new BinanceSpotKlineSyncConfigProperties();
        var h=new IntervalSyncConfig();h.setMinMaintainCount(history);h.setListenSymbolPatterns(List.of(".*"));
        var d=new IntervalSyncConfig();d.setMinMaintainCount(history);d.setListenSymbolPatterns(List.of(".*"));
        config.setIntervalSyncConfigs(Map.of("1h",h,"1d",d));set(s,"klineSyncConfigProperties",config);service=s;
      }
      set(service,"serializer",SERIALIZER);set(service,"numberType","double");
      set(service,"monitorManager",new MonitorManagerImpl(new CollectorRegistry()));
      set(service,"closedBarLatencyEnabled",diagnostics);
      var persistence=new KlinePersistenceProperties();persistence.setEnabled(true);
      var pi=new KlinePersistenceProperties.IntervalPersistenceConfig();pi.setMaxStoreCount(history);
      (future?persistence.getFuture():persistence.getSpot()).setIntervalConfigs(Map.of("1h",pi,"1d",pi));
      // Persistence store work is not scheduled; keep the actual dirty-key hot path enabled below.
      set(service,"persistenceProperties",persistence);
      var bulk=new KlineBulkProperties();bulk.setFinalWaitMaxMs(8_000L);set(service,"bulkProperties",bulk);
      for(String symbol:symbols) {
        for(String interval:List.of("1h","1d")) {
          long duration=interval.equals("1h")?H:D;
          long latest=interval.equals("1h")?BASE-H:BASE/D*D;
          var key=new KlineSetKey(symbol,interval);var ks=new KlineSet(key);
          for(int j=history-1;j>=0;j--) {
            long open=latest-j*duration;var bar=bar(open,duration,1,100);
            ks.getKlineMap().put(open,bar);if(j!=0)ks.markFinal(open);
          }
          service.klineSetMap.put(key,ks);
        }
      }
      service.getKlineStreams().forEach(stream->stream.subscriptionState());
      client=new Client(future?"future":"spot",service.getKlineEventMessageHandler(),service.getKlineEventMessageTopicExtractor());
      try {
        var classifier=AbstractKlineService.class.getDeclaredMethod("getKlineMessageClassifier");classifier.setAccessible(true);
        var setter=AbstractWebSocketClient.class.getMethod("setKlineMessageClassifier",Function.class);
        setter.invoke(client,classifier.invoke(service));
      }catch(NoSuchMethodException baseline){/* Baseline predates ingress classification. */}
      catch(Exception e){throw new RuntimeException(e);}
    }
  }

  static Kline.DoubleKline bar(long open,long interval,int n,double close) {
    var k=new Kline.DoubleKline();k.setOpenTime(open);k.setCloseTime(open+interval-1);k.setTradeNum(n);
    k.setOpenPrice(100);k.setHighPrice(110);k.setLowPrice(90);k.setClosePrice(close);
    k.setVolume(1000);k.setQuoteVolume(100000);k.setActiveBuyVolume(500);k.setActiveBuyQuoteVolume(50000);return k;
  }

  static class Client extends AbstractWebSocketClient<Long> {
    final String market;
    static final MethodHandle DIRECT;
    static {
      MethodHandle direct = null;
      try {direct=MethodHandles.privateLookupIn(AbstractWebSocketClient.class,MethodHandles.lookup())
          .findVirtual(AbstractWebSocketClient.class,"handleMessage",MethodType.methodType(void.class,String.class,WebSocketMessageTiming.class));}
      catch(NoSuchMethodException currentImplementation){/* direct entry belongs only to the baseline prototype */}
      catch(Exception e){throw new ExceptionInInitializerError(e);}
      DIRECT=direct;
    }
    Client(String market,Function<ParsedWebSocketMessage,Boolean> handler,Function<ParsedWebSocketMessage,String> topic) {
      this.market=market;this.serializer=SERIALIZER;this.clientMonitorTask=new ClientMonitorTask(this);
      addMessageTopicExtractorHandler(topic);
      addMessageHandler(message->{
        boolean ok=handler.apply(message);
        var round=current;
        if(ok&&round!=null){round.handled.increment();
          if(message.payloadNode().path("k").path("x").asBoolean()) {
            String symbol=message.payloadNode().path(market.equals("future")?"ps":"s").asText();
            round.closeTimes.putIfAbsent(market+symbol,ms(System.nanoTime()-round.startNanos)-20);
            round.closedHandled.increment();
          }
        }
        return ok;
      });
    }
    void direct(String raw,WebSocketMessageTiming timing) {
      if(DIRECT==null)throw new IllegalStateException("Research mailbox prototype requires the pinned baseline implementation");
      timing.handlerStarted(0);
      try {DIRECT.invokeExact((AbstractWebSocketClient)this,raw,timing);}
      catch(Throwable e){throw new RuntimeException(e);}
    }
    public String clientName(){return "replay-"+market;}
    protected WebSocketFrame buildSubscribeFrame(Collection<String> t){return new TextWebSocketFrame();}
    protected WebSocketFrame buildUnsubscribeFrame(Collection<String> t){return new TextWebSocketFrame();}
    protected WebSocketFrame buildListTopicsFrame(){return new TextWebSocketFrame();}
    protected int getMaxTopicsPerTime(){return 100;}
    protected int getMaxFramesPerSecond(){return 5;}
    protected boolean monitorTopicMessage(){return true;}
    protected String getUrl(){return "wss://unused.invalid";}
    protected Long generateSubId(){return 1L;}
    protected Long generateId(){return 1L;}
  }

  record Input(Client client,String raw,double atMs){}
  static String raw(boolean future,int index,String symbol,long open,boolean closed,int n,double close,long event) {
    String head=future?"\"e\":\"continuous_kline\",\"ps\":\""+symbol+"\",\"ct\":\""+(index<528?"PERPETUAL":"TRADIFI_PERPETUAL")+"\"":
        "\"e\":\"kline\",\"s\":\""+symbol+"\"";
    return "{"+head+",\"E\":"+event+",\"k\":{\"t\":"+open+",\"T\":"+(open+H-1)+",\"i\":\"1h\",\"f\":1,\"L\":"+n+
        ",\"o\":\"100.0\",\"h\":\"110.0\",\"l\":\"90.0\",\"c\":\""+close+"\",\"v\":\"1000.0\",\"q\":\"100000.0\",\"V\":\"500.0\",\"Q\":\"50000.0\",\"n\":"+n+",\"x\":"+closed+"}}";
  }
  static List<Input> inputs(List<Fixture> fixtures,long boundary,int updates,boolean flood) {
    var result=new ArrayList<Input>();
    for(Fixture f:fixtures)for(int i=0;i<f.symbols.size();i++) {
      String symbol=f.symbols.get(i);
      for(int wave=0;wave<updates;wave++)result.add(new Input(f.client,raw(f.future,i,symbol,boundary-H,false,2+wave,101+wave,boundary-10+wave),flood?0:-10+wave*.1));
      long event=f.future?(i<528?106:0):21+i%84;
      double recv=f.future?(i<528?121+i%11:5+i%11):event+8;
      for(int copy=0;copy<Integer.getInteger("closeCopies",1);copy++)
        result.add(new Input(f.client,raw(f.future,i,symbol,boundary-H,true,2+updates,109,boundary+event),flood?0:recv));
      int post=flood?updates:1;
      for(int wave=0;wave<post;wave++)result.add(new Input(f.client,raw(f.future,i,symbol,boundary,false,1+wave,101+wave,boundary+150+wave),flood?0:160+wave*.1));
    }
    // Stable tie order intentionally interleaves per-symbol updates during flood, allowing final eviction.
    result.sort(Comparator.comparingDouble(Input::atMs));return result;
  }

  static final class Round {
    final long startNanos=System.nanoTime();
    final LongAdder handled=new LongAdder(),closedHandled=new LongAdder();
    final Map<String,Double> closeTimes=new ConcurrentHashMap<>();
  }
  @Name("proxy.ReplayRound") @Label("Replay round") @Category("Proxy")
  static class RoundEvent extends Event {int round;boolean warmup;String variant;}

  /** Streaming JSON header scan: includes its cost before mailbox publication. Raw and combined objects. */
  record Header(String identity,String interval,long open,boolean closed) {
    static Header parse(String raw) {
      String symbol=null,contract="",interval=null;long open=0;boolean closed=false;
      try(JsonParser p=JSON.getFactory().createParser(raw)) {
        while(p.nextToken()!=null)if(p.currentToken()==JsonToken.FIELD_NAME) {
          String name=p.currentName();p.nextToken();
          switch(name){
            case "s","ps" -> symbol=p.getValueAsString();
            case "ct" -> contract=p.getValueAsString();
            case "i" -> interval=p.getValueAsString();
            case "t" -> open=p.getLongValue();
            case "x" -> closed=p.getBooleanValue();
            default -> { /* Preserve traversal of data/k; primitive fields need no allocation. */ }
          }
        }
      }catch(Exception e){throw new RuntimeException(e);}
      if(symbol==null||interval==null)throw new IllegalArgumentException("research harness only accepts kline frames");
      return new Header(symbol+"/"+contract,interval,open,closed);
    }
  }
  record Key(String market,String identity,String interval,long open){}
  record Envelope(Client client,String raw,WebSocketMessageTiming timing,Header header){}
  static class Slot {Envelope latest;boolean queued,closed;}
  /** Prototype: single writer per key, final FIFO is never evicted, forming snapshots coalesce.
   * Unbounded final queue is only for the bounded experiment, not a production overload policy.
   */
  static class Mailboxes implements AutoCloseable {
    final Shard[] shards;
    final LongAdder merged=new LongAdder(),suppressed=new LongAdder(),headerNanos=new LongAdder();
    Mailboxes(int workers){shards=new Shard[workers];for(int i=0;i<workers;i++){shards[i]=new Shard(i);shards[i].start();}}
    void offer(Input input) {
      var timing=new WebSocketMessageTiming(input.client.clientName(),System.currentTimeMillis(),System.nanoTime());
      input.client.onReceiveNoHandle();
      long start=System.nanoTime();Header h=Header.parse(input.raw);headerNanos.add(System.nanoTime()-start);
      var key=new Key(input.client.market,h.identity,h.interval,h.open);
      var shard=shards[Math.floorMod(Objects.hash(key.market,key.identity,key.interval),shards.length)];
      var slot=shard.slots.computeIfAbsent(key,k->new Slot());
      var env=new Envelope(input.client,input.raw,timing,h);
      timing.enqueued(shard.finals.size()+shard.normal.size());
      synchronized(slot){
        if(h.closed){slot.closed=true;if(slot.latest!=null){slot.latest=null;suppressed.increment();}shard.finals.add(env);shard.wake.release();}
        else if(slot.closed){suppressed.increment();}
        else {if(slot.latest!=null)merged.increment();slot.latest=env;if(!slot.queued){slot.queued=true;shard.normal.add(key);shard.wake.release();}}
      }
    }
    boolean idle(){return Arrays.stream(shards).allMatch(s->s.wake.availablePermits()==0&&s.busy.get()==0);}
    public void close(){for(Shard s:shards)s.interrupt();for(Shard s:shards)try{s.join(2000);}catch(InterruptedException e){Thread.currentThread().interrupt();}}
    class Shard extends Thread {
      final Map<Key,Slot> slots=new ConcurrentHashMap<>();
      final Queue<Envelope> finals=new ConcurrentLinkedQueue<>();
      final Queue<Key> normal=new ConcurrentLinkedQueue<>();
      final Semaphore wake=new Semaphore(0);final AtomicInteger busy=new AtomicInteger();
      Shard(int i){super("mailbox-"+i);}
      public void run(){
        try{while(true){wake.acquire();busy.incrementAndGet();try{
          Envelope e=finals.poll();
          if(e==null){Key key=normal.poll();if(key!=null){Slot slot=slots.get(key);synchronized(slot){e=slot.closed?null:slot.latest;slot.latest=null;slot.queued=false;}}}
          if(e!=null)e.client.direct(e.raw,e.timing);
        }finally{busy.decrementAndGet();}}}catch(InterruptedException e){/* experiment shutdown */}
      }
    }
  }

  static Map<String,Object> runRound(List<Fixture> fixtures,Mailboxes boxes,int round,int updates,boolean flood,int bulkWaiters,boolean warmup,String variant) throws Exception {
    long boundary=BASE+round*H;
    List<Input> inputs=inputs(fixtures,boundary,updates,flood);
    current=new Round();Round state=current;clockBoundary=boundary;clockStart=state.startNanos;
    var dispatcherBefore=dispatcherStats();
    long cpuBefore=OS.getProcessCpuTime(),dropBefore=DROPS.get();
    long mergeBefore=boxes==null?0:boxes.merged.sum(),suppressedBefore=boxes==null?0:boxes.suppressed.sum(),headerBefore=boxes==null?0:boxes.headerNanos.sum();
    var event=new RoundEvent();event.round=round;event.warmup=warmup;event.variant=variant;event.begin();
    var waitResults=new ConcurrentLinkedQueue<BulkKlinesResponse>();
    ExecutorService readers=READERS;
    var bulkStart=new CountDownLatch(1);
    if(bulkWaiters>0)BULK_STARTER.schedule(bulkStart::countDown,
        Math.max(0,25_000_000L-(System.nanoTime()-state.startNanos)),TimeUnit.NANOSECONDS);
    var bulkDoneTimes=new ConcurrentLinkedQueue<Double>();
    var futures=new ArrayList<Future<?>>();
    Fixture future=fixtures.getFirst();
    for(int i=0;i<bulkWaiters;i++) {
      int index=i;
      futures.add(readers.submit(()->{
        try{bulkStart.await();}catch(InterruptedException e){throw new RuntimeException(e);}
        var requested=new ArrayList<String>();for(int j=0;j<6;j++)requested.add(future.symbols.get((index*7+j)%future.symbols.size()));
        waitResults.add(future.service.queryBulkKlines("1h",1,true,requested));
        bulkDoneTimes.add(ms(System.nanoTime()-state.startNanos)-20);
      }));
    }
    for(Input input:inputs) {
      long at=state.startNanos+(long)((input.atMs+20)*1e6);
      if(!flood){long remaining;while((remaining=at-System.nanoTime())>0)LockSupport.parkNanos(Math.min(remaining,200_000));}
      if(boxes==null)input.client.onReceive(input.raw);else boxes.offer(input);
    }
    double enqueueEnd=ms(System.nanoTime()-state.startNanos)-20;
    long deadline=System.nanoTime()+20_000_000_000L;
    while(System.nanoTime()<deadline){
      boolean idle=boxes==null?FIFO.getQueue().isEmpty()&&FIFO.getActiveCount()==0&&productionIdle():boxes.idle();
      if(idle){Thread.sleep(10);if(boxes==null?FIFO.getQueue().isEmpty()&&FIFO.getActiveCount()==0&&productionIdle():boxes.idle())break;}
      Thread.sleep(1);
    }
    double drained=ms(System.nanoTime()-state.startNanos)-20;
    for(Future<?> f:futures)f.get(10,TimeUnit.SECONDS);
    double cpuMs=ms(OS.getProcessCpuTime()-cpuBefore);
    event.end();event.commit();
    int finalMissing=0,finalWrong=0,formingWrong=0,expected=0;
    for(Fixture f:fixtures)for(String symbol:f.symbols) {
      expected++;KlineSet ks=f.service.klineSetMap.get(new KlineSetKey(symbol,"1h"));
      Kline k=ks.getKlineMap().get(boundary-H);
      if(!ks.isFinal(boundary-H))finalMissing++;
      if(k==null||k.getTradeNum()!=2+updates||((Kline.DoubleKline)k).getClosePrice()!=109)finalWrong++;
      Kline forming=ks.getKlineMap().get(boundary);
      if(forming==null||forming.getTradeNum()!=(flood?updates:1))formingWrong++;
    }
    Map<String,Object> result=new LinkedHashMap<>();
    result.put("variant",variant);result.put("round",round);result.put("warmup",warmup);result.put("updates_per_old_bar",updates);result.put("input",inputs.size());
    result.put("enqueue_end_ms",enqueueEnd);result.put("drained_ms",drained);result.put("cpu_ms",cpuMs);
    result.put("close_done_p50_ms",quantile(state.closeTimes.values(),.5));result.put("close_done_p90_ms",quantile(state.closeTimes.values(),.9));result.put("close_done_max_ms",quantile(state.closeTimes.values(),1));
    result.put("expected_closes",expected);result.put("handled_closes",state.closedHandled.sum());result.put("handled_all",state.handled.sum());
    result.put("expected_close_messages",expected*Integer.getInteger("closeCopies",1));
    result.put("executor_drops",DROPS.get()-dropBefore);result.put("executor_largest_pool",FIFO.getLargestPoolSize());result.put("missing_finals",finalMissing);result.put("wrong_final_values",finalWrong);result.put("wrong_latest_forming",formingWrong);
    result.put("bulk_responses",waitResults.size());result.put("bulk_unfinalized",waitResults.stream().filter(x->!x.finalized()).count());
    result.put("bulk_done_p50_ms",quantile(bulkDoneTimes,.5));result.put("bulk_done_p90_ms",quantile(bulkDoneTimes,.9));result.put("bulk_done_max_ms",quantile(bulkDoneTimes,1));
    result.put("merged_forming",boxes==null?0:boxes.merged.sum()-mergeBefore);result.put("suppressed_forming",boxes==null?0:boxes.suppressed.sum()-suppressedBefore);
    result.put("header_scan_cpu_elapsed_sum_ms",boxes==null?0:ms(boxes.headerNanos.sum()-headerBefore));
    var dispatcherAfter=dispatcherStats();
    for(var metric:dispatcherAfter.entrySet())result.put("ingress_"+metric.getKey(),metric.getValue()-dispatcherBefore.getOrDefault(metric.getKey(),0L));
    current=null;return result;
  }

  static Map<String,Object> retentionSnapshot(List<Fixture> fixtures) {
    long bars=0,versions=0,finals=0,orphanVersions=0,orphanFinals=0;
    int maxBars=0,maxVersions=0;
    for(Fixture f:fixtures)for(KlineSet set:f.service.klineSetMap.values()) {
      var map=set.getKlineMap();bars+=map.size();maxBars=Math.max(maxBars,map.size());
      var finalTimes=set.getFinalOpenTimes();finals+=finalTimes.size();
      orphanFinals+=finalTimes.stream().filter(x->!map.containsKey(x)).count();
      try {
        Map<?,?> stamps=(Map<?,?>)field(KlineSet.class,set,"streamVersions");
        versions+=stamps.size();maxVersions=Math.max(maxVersions,stamps.size());
        orphanVersions+=stamps.keySet().stream().filter(x->!map.containsKey(x)).count();
      }catch(IllegalArgumentException baseline){/* no stream version map in baseline */}
    }
    var result=new LinkedHashMap<String,Object>();result.put("bars",bars);result.put("stream_versions",versions);
    result.put("final_flags",finals);result.put("max_bars_per_series",maxBars);result.put("max_versions_per_series",maxVersions);
    result.put("orphan_versions",orphanVersions);result.put("orphan_final_flags",orphanFinals);
    result.put("heap_used_bytes",ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());return result;
  }

  public static void main(String[] args) throws Exception {
    String variant=System.getProperty("variant","baseline");
    String queue=System.getProperty("queue","fifo");
    int workers=Integer.getInteger("workers",4),warmups=Integer.getInteger("warmups",3),runs=Integer.getInteger("runs",5);
    int updates=Integer.getInteger("updates",1),history=Integer.getInteger("history",1000),waiters=Integer.getInteger("waiters",0);
    boolean flood=Boolean.getBoolean("flood"),diagnostics=Boolean.parseBoolean(System.getProperty("diagnostics","true"));
    ((Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.ERROR);
    FIFO.setMaximumPoolSize(Math.max(workers,20));FIFO.setCorePoolSize(workers);FIFO.prestartAllCoreThreads();
    READERS=(ThreadPoolExecutor)Executors.newFixedThreadPool(Math.max(1,waiters));
    if(waiters>0)READERS.prestartAllCoreThreads();
    var fixtures=List.of(new Fixture(true,Integer.getInteger("futureSymbols",718),history,diagnostics),
        new Fixture(false,Integer.getInteger("spotSymbols",490),history,diagnostics));
    Mailboxes boxes=queue.equals("mailbox")?new Mailboxes(workers):null;
    Recording recording=null;
    String jfr=System.getProperty("jfr");
    if(jfr!=null){recording=new Recording(Configuration.getConfiguration("profile"));
      recording.enable("jdk.ExecutionSample").withPeriod(Duration.ofMillis(2)).withStackTrace();
      recording.enable("jdk.JavaMonitorEnter").withThreshold(Duration.ofMillis(1)).withStackTrace();
      recording.enable("jdk.ThreadPark").withThreshold(Duration.ofMillis(1)).withStackTrace();
      recording.enable(RoundEvent.class);recording.start();}
    var results=new ArrayList<Map<String,Object>>();
    try{
      for(int round=0;round<warmups+runs;round++){
        var result=runRound(fixtures,boxes,round,updates,flood,waiters,round<warmups,variant);results.add(result);
        if(Boolean.getBoolean("soak") && (round % 100 == 0 || round == warmups+runs-1)) {
          System.gc();Thread.sleep(100);
          result.put("retention",retentionSnapshot(fixtures));
        }
        System.out.println(JSON.writeValueAsString(result));
      }
      Map<String,Object> output=new LinkedHashMap<>();output.put("java",System.getProperty("java.runtime.version"));output.put("arch",System.getProperty("os.arch"));
      output.put("available_processors",Runtime.getRuntime().availableProcessors());output.put("history",history);output.put("workers",workers);output.put("queue",queue);output.put("diagnostics",diagnostics);output.put("waiters",waiters);output.put("rounds",results);
      Files.writeString(Path.of(args[0]),JSON.writerWithDefaultPrettyPrinter().writeValueAsString(output));
    }finally{if(recording!=null){recording.stop();recording.dump(Path.of(jfr));recording.close();}if(boxes!=null)boxes.close();READERS.shutdown();BULK_STARTER.shutdown();FIFO.shutdown();}
  }
}
