package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.model.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import ch.qos.logback.classic.*;
import org.slf4j.LoggerFactory;

/** Deterministic counterexamples against unchanged production cache methods. */
public class CorrectnessProbe {
  static final long OPEN=ReplayHarness.BASE-ReplayHarness.H;
  static final class BlockingMap extends ConcurrentSkipListMap<Long,Kline> {
    final CountDownLatch oldAboutToPut=new CountDownLatch(1), newerCompleted=new CountDownLatch(1);
    volatile boolean armed;
    @Override public Kline put(Long key,Kline value) {
      if(armed&&key==OPEN&&value.getTradeNum()==10) {
        oldAboutToPut.countDown();
        try { if(!newerCompleted.await(5,TimeUnit.SECONDS))throw new AssertionError("newer did not complete"); }
        catch(InterruptedException e){throw new AssertionError(e);}
      }
      return super.put(key,value);
    }
  }
  static Map<String,Object> snapshot(String scenario,KlineSet set,int expectedN,double expectedPrice) {
    var k=(Kline.DoubleKline)set.getKlineMap().get(OPEN);
    return Map.of("scenario",scenario,"expected_trade_num",expectedN,"actual_trade_num",k.getTradeNum(),
        "expected_close",expectedPrice,"actual_close",k.getClosePrice(),"is_final",set.isFinal(OPEN),
        "correct",k.getTradeNum()==expectedN&&k.getClosePrice()==expectedPrice);
  }
  public static void main(String[] args)throws Exception {
    ((Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.ERROR);
    var f=new ReplayHarness.Fixture(true,1,10,false);String symbol=f.symbols.getFirst();
    var key=new KlineSetKey(symbol,"1h");var results=new ArrayList<Map<String,Object>>();
    KlineSet set=new KlineSet(key);set.getKlineMap().put(OPEN,ReplayHarness.bar(OPEN,ReplayHarness.H,10,101));
    f.service.klineSetMap.put(key,set);
    f.service.updateStreamKline(symbol,"1h",ReplayHarness.bar(OPEN,ReplayHarness.H,10,109),true);
    results.add(snapshot("same_n_final_snapshot",set,10,109));
    for(boolean closed:List.of(false,true)) {
      var blocking=new BlockingMap();blocking.put(OPEN,ReplayHarness.bar(OPEN,ReplayHarness.H,1,100));blocking.armed=true;
      set=new KlineSet(key);set.setKlineMap(blocking);f.service.klineSetMap.put(key,set);
      var pool=Executors.newFixedThreadPool(2);
      Future<?> older=pool.submit(()->f.service.updateStreamKline(symbol,"1h",ReplayHarness.bar(OPEN,ReplayHarness.H,10,101),false));
      if(!blocking.oldAboutToPut.await(5,TimeUnit.SECONDS))throw new AssertionError("older did not reach put");
      f.service.updateStreamKline(symbol,"1h",ReplayHarness.bar(OPEN,ReplayHarness.H,20,109),closed);
      blocking.newerCompleted.countDown();older.get(5,TimeUnit.SECONDS);pool.shutdown();
      results.add(snapshot(closed?"inflight_forming_overwrites_final":"inflight_older_overwrites_newer",set,20,109));
    }
    Files.writeString(Path.of(args[0]),ReplayHarness.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(results));
    System.out.println(ReplayHarness.JSON.writeValueAsString(results));ReplayHarness.FIFO.shutdown();
    if(results.stream().anyMatch(r->Boolean.TRUE.equals(r.get("correct"))))throw new AssertionError("expected all counterexamples to reproduce on baseline");
  }
}
