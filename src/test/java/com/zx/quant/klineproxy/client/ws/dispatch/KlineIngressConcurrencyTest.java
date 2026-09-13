package com.zx.quant.klineproxy.client.ws.dispatch;

import com.zx.quant.klineproxy.model.KlineDispatchMetadata;
import static org.assertj.core.api.Assertions.assertThat;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSet;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.KlineUpdateSource;
import com.zx.quant.klineproxy.model.config.KlineIngressProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class KlineIngressConcurrencyTest {
  private record Input(KlineDispatchMetadata metadata, long sequence, Kline.StringKline value) { }

  @Test
  void interleavedMarketsIntervalsAndHoursSurviveTinyQueuesAndClosedKeyEviction() throws Exception {
    List<Input> inputs = new ArrayList<>();
    Map<KlineDispatchMetadata.Bar, KlineSet> stores = new HashMap<>();
    Map<KlineDispatchMetadata.Bar, Input> expected = new HashMap<>();
    Comparator<Input> finalOrder = Comparator.comparingInt((Input i) -> i.metadata.tradeCount())
        .thenComparingLong(i -> i.metadata.eventTime()).thenComparingLong(Input::sequence);
    long sequence = 0;
    for (String market : List.of("spot", "future")) {
      for (String interval : List.of("1h", "1d")) {
        for (String symbol : List.of("BTCUSDT", "ETHUSDT", "PINGUSDT", "PONGUSDT", "OTHERUSDT")) {
          var series = new KlineDispatchMetadata.Series(market, symbol, interval);
          for (long open = 0; open < 3; open++) {
            var key = new KlineDispatchMetadata.Bar(series, open);
            stores.put(key, new KlineSet(new KlineSetKey(symbol, interval)));
            for (int i = 0; i < 100; i++) {
              sequence++;
              boolean closed = (i % 3) == 0;
              var metadata = new KlineDispatchMetadata(series, open, closed, i / 4, (long) i / 2, "topic");
              var value = new Kline.StringKline();
              value.setOpenTime(open);
              value.setCloseTime(open + 1);
              value.setTradeNum(i / 4);
              value.setClosePrice(Long.toString(sequence));
              Input input = new Input(metadata, sequence, value);
              inputs.add(input);
              if (closed) {
                expected.merge(key, input, (a, b) -> finalOrder.compare(a, b) < 0 ? b : a);
              }
            }
          }
        }
      }
    }
    Collections.shuffle(inputs, new Random(5946)); // deterministic out-of-order/reconnect overlap
    var properties = new KlineIngressProperties();
    properties.setWorkers(4);
    properties.setFinalQueueCapacityPerWorker(2);
    properties.setFormingQueueCapacityPerWorker(2);
    properties.setClosedKeyCapacityPerWorker(1); // force eviction; storage must still protect finals
    try (var dispatcher = new KlineMessageDispatcher(properties);
        var producers = Executors.newFixedThreadPool(16)) {
      var submissions = inputs.stream().map(input -> producers.submit(() -> {
        dispatcher.submit(input.metadata, input.sequence,
            () -> stores.get(input.metadata.bar()).commit(input.value, input.metadata.closed(),
                KlineUpdateSource.STREAM, input.metadata.eventTime(), input.sequence));
      })).toList();
      for (var submission : submissions) {
        submission.get(10, TimeUnit.SECONDS);
      }
      assertThat(dispatcher.shutdown(Duration.ofSeconds(5))).isTrue();
      long finals = inputs.stream().filter(input -> input.metadata.closed()).count();
      assertThat(dispatcher.snapshot().receivedFinal()).isEqualTo(finals);
      assertThat(dispatcher.snapshot().processedFinal()).isEqualTo(finals);
      assertThat(dispatcher.snapshot().failures()).isZero();
      assertThat(dispatcher.snapshot().backpressureCount()).isPositive();
      for (var entry : expected.entrySet()) {
        KlineSet store = stores.get(entry.getKey());
        assertThat(store.isFinal(entry.getKey().openTime())).isTrue();
        assertThat(store.getKlineMap().get(entry.getKey().openTime())).isEqualTo(entry.getValue().value);
      }
    }
  }
}
