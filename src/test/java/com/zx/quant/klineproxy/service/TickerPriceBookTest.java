package com.zx.quant.klineproxy.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker.BigDecimalTicker;
import com.zx.quant.klineproxy.model.Ticker.StringTicker;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

class TickerPriceBookTest {

  private static final long STALE = 2_000L;
  private static final long GAP = 500L;
  private static final long REST_LAG = 1_000L;

  private final TickerPriceBook book = new TickerPriceBook(GAP, REST_LAG);

  @Test
  void streamOnlyUpdatesSymbolsASnapshotListedAndOnlyWhenNewer() {
    assertThat(book.updateFromStream("BTCUSDT", new BigDecimal("100"), 1_000L)).as("before any snapshot").isFalse();

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_000L);

    assertThat(book.updateFromStream("BTCUSD_PERP", new BigDecimal("1"), 9_900L)).as("not listed").isFalse();
    assertThat(book.updateFromStream("BTCUSDT", new BigDecimal("99"), 999L)).isFalse();
    assertThat(book.updateFromStream("BTCUSDT", new BigDecimal("98"), 1_000L)).isFalse();
    assertThat(book.updateFromStream("BTCUSDT", new BigDecimal("101"), 1_001L)).isTrue();

    assertThat(book.all()).extracting(Ticker::getSymbol).containsExactly("BTCUSDT");
    assertThat(price("BTCUSDT")).isEqualTo("101");
  }

  @Test
  void streamUpdatesBeforeTheFirstSnapshotAreReplayedForTheSymbolsItLists() {
    book.updateFromStream("QUIET", new BigDecimal("110"), 9_900L);
    book.updateFromStream("QUIET", new BigDecimal("109"), 9_800L);  // older, handled late
    book.updateFromStream("BTCUSD_PERP", new BigDecimal("77000"), 9_900L);

    // the snapshot trails the stream: its QUIET price predates the stream update
    book.applySnapshot(List.of(dated("QUIET", "100", 9_500L)), 10_050L);

    assertThat(price("QUIET")).isEqualTo("110");
    assertThat(book.all()).extracting(Ticker::getSymbol).containsExactly("QUIET");
    assertThat(book.contains("BTCUSD_PERP")).isFalse();
  }

  @Test
  void aPendingUpdateNewerThanAnOmittingSnapshotSurvivesIt() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 9_000L);
    book.updateFromStream("NEWUSDT", new BigDecimal("8"), 10_100L);  // listed while the next snapshot is in flight

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_000L);  // covers up to 9_000, omits NEWUSDT
    book.applySymbols(List.of(dated("NEWUSDT", "7", 10_010L)));

    assertThat(price("NEWUSDT")).isEqualTo("8");
  }

  @Test
  void aPendingUpdateThatASnapshotCoversAndOmitsIsDropped() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 9_000L);
    book.updateFromStream("GONEUSDT", new BigDecimal("5"), 8_500L);

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_000L);  // covers up to 9_000
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L), dated("GONEUSDT", "4", 8_000L)), 11_000L);

    assertThat(price("GONEUSDT")).as("only the snapshot's own price").isEqualTo("4");
  }

  @Test
  void aSymbolOutsideTheBookJoinsOnlyWithAPriceNewerThanTheLatestSnapshot() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 12_000L);  // covers up to 11_000

    book.applySymbols(List.of(dated("GONEUSDT", "1", 9_500L)));  // a lookup started before the snapshot
    assertThat(book.contains("GONEUSDT")).isFalse();

    book.applySymbols(List.of(dated("NEWUSDT", "7", 11_500L)));
    assertThat(price("NEWUSDT")).isEqualTo("7");
  }

  @Test
  void aDatedSymbolLookupAdmitsASymbolAndReplaysItsPendingUpdate() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_000L);
    book.updateFromStream("NEWUSDT", new BigDecimal("8"), 9_800L);
    assertThat(book.contains("NEWUSDT")).isFalse();

    book.applySymbols(List.of(dated("NEWUSDT", "7", 9_500L)));

    assertThat(price("NEWUSDT")).isEqualTo("8");
  }

  @Test
  void anOlderSnapshotFinishingLateNeverAddsSymbols() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 12_000L);

    book.applySnapshot(List.of(dated("BTCUSDT", "101", 10_500L), dated("GONEUSDT", "1", 9_000L)), 11_000L);

    assertThat(book.all()).extracting(Ticker::getSymbol).containsExactly("BTCUSDT");
    assertThat(price("BTCUSDT")).as("existing symbols still take newer prices").isEqualTo("101");
  }

  @Test
  void anAnswerWithoutAPriceRemovesOnlyEntriesTheRequestIsNewerThan() {
    book.applySnapshot(List.of(dated("SETTLINGUSDT", "5", 1_000L), dated("BTCUSDT", "100", 1_000L)), 10_000L);
    book.updateFromStream("BTCUSDT", new BigDecimal("101"), 9_500L);  // newer than the request below

    book.applyAbsent(List.of("SETTLINGUSDT", "BTCUSDT"), 10_000L);  // covers up to 9_000

    assertThat(book.all()).extracting(Ticker::getSymbol).containsExactly("BTCUSDT");
  }

  @Test
  void datedRestAndStreamMergeToTheNewestWhateverTheArrivalOrder() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_000L);

    // REST newer than the stream event, stream event handled late
    book.applySymbols(List.of(dated("BTCUSDT", "110", 9_800L)));
    book.updateFromStream("BTCUSDT", new BigDecimal("105"), 9_500L);
    assertThat(price("BTCUSDT")).isEqualTo("110");

    // stream newer than REST, REST response arriving late
    book.updateFromStream("BTCUSDT", new BigDecimal("120"), 9_900L);
    book.applySymbols(List.of(dated("BTCUSDT", "111", 9_850L)));
    assertThat(price("BTCUSDT")).isEqualTo("120");
  }

  @Test
  void undatedRestPricesAreNeverMerged() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_000L);

    book.applySymbols(List.of(dated("BTCUSDT", "999", 0L), dated("NEWUSDT", "1", 0L)));
    book.applySnapshot(List.of(dated("BTCUSDT", "999", 0L)), 20_000L);

    assertThat(price("BTCUSDT")).isEqualTo("100");
    assertThat(book.contains("NEWUSDT")).isFalse();
  }

  @Test
  void restPricesOfAnyNumberTypeAreAccepted() {
    StringTicker ticker = new StringTicker();
    ticker.setSymbol("BTCUSDT");
    ticker.setPrice("100.50");
    ticker.setTime(1_000L);

    book.applySnapshot(List.of(ticker), 10_000L);

    assertThat(price("BTCUSDT")).isEqualTo("100.50");
  }

  @Test
  void snapshotRemovesUnlistedSymbolsOnlyWhenItCoversTheirLastUpdate() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L), dated("GONEUSDT", "1", 1_000L)), 10_000L);
    book.applySymbols(List.of(dated("NEWUSDT", "7", 9_500L)));  // listed after the next snapshot was taken

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_200L);  // covers up to 9_200

    assertThat(book.all()).extracting(Ticker::getSymbol).containsExactly("BTCUSDT", "NEWUSDT");
  }

  @Test
  void aRemovedSymbolComesBackOnlyThroughNewerData() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L), dated("GONEUSDT", "1", 1_000L)), 10_000L);
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 11_000L);  // removes GONEUSDT at 10_000

    book.applySymbols(List.of(dated("GONEUSDT", "1", 9_000L)));  // response issued before the removal
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L), dated("GONEUSDT", "1", 1_000L)), 10_500L);
    assertThat(book.contains("GONEUSDT")).as("stale data").isFalse();

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L), dated("GONEUSDT", "2", 11_500L)), 12_000L);
    assertThat(price("GONEUSDT")).as("relisted by a later snapshot").isEqualTo("2");
  }

  @Test
  void anOlderSnapshotFinishingLateDoesNotChangeMembership() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L), dated("ETHUSDT", "50", 1_000L)), 12_000L);

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 11_000L);

    assertThat(book.all()).extracting(Ticker::getSymbol).containsExactly("BTCUSDT", "ETHUSDT");
  }

  @Test
  void allMarketListIsSortedAndReusedUntilTheBookChanges() {
    book.applySnapshot(List.of(dated("ETHUSDT", "50", 1_000L), dated("BTCUSDT", "100", 1_000L)), 10_000L);

    List<Ticker<?>> first = book.all();
    assertThat(first).extracting(Ticker::getSymbol).containsExactly("BTCUSDT", "ETHUSDT");
    assertThat(book.all()).isSameAs(first);

    book.updateFromStream("BTCUSDT", new BigDecimal("100"), 900L);  // rejected: nothing changed
    assertThat(book.all()).isSameAs(first);

    book.updateFromStream("BTCUSDT", new BigDecimal("101"), 1_100L);
    assertThat(book.all()).isNotSameAs(first);
  }

  @Test
  void getKeepsRequestOrderAndSkipsUnknownSymbols() {
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L), dated("ETHUSDT", "50", 1_000L)), 10_000L);

    assertThat(book.get(List.of("ETHUSDT", "XRPUSDT", "BTCUSDT")))
        .extracting(Ticker::getSymbol).containsExactly("ETHUSDT", "BTCUSDT");
  }

  @Test
  void coveredOnlyWhenASnapshotCoversTheSegmentStart() {
    assertThat(book.isCovered()).isFalse();
    assertThat(book.hasFullSnapshot()).isFalse();

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_000L);  // covers up to 9_000
    assertThat(book.hasFullSnapshot()).isTrue();

    assertThat(book.onStreamFrame(9_100L, 10_000L)).isTrue();  // segment starts at 9_100
    assertThat(book.isCovered()).as("snapshot predates the segment").isFalse();
    assertThat(book.needsFullSync()).isTrue();

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 10_100L);  // covers up to 9_100
    assertThat(book.isCovered()).isTrue();
    assertThat(book.needsFullSync()).isFalse();
  }

  @Test
  void streamIsAliveUntilItStaysSilentLongerThanTheThreshold() {
    assertThat(book.isStreamAlive(10_000L, STALE)).isFalse();

    book.onStreamFrame(9_000L, 10_000L);

    assertThat(book.isStreamAlive(10_000L + STALE, STALE)).isTrue();
    assertThat(book.isStreamAlive(10_000L + STALE + 1L, STALE)).isFalse();
    assertThat(book.isStreamAlive(10_000L + 30_000L, 30_000L)).isTrue();
  }

  @Test
  void contiguousFramesContinueTheSegmentAndAJumpStartsANewOne() {
    assertThat(book.onStreamFrame(9_010L, 9_990L)).isTrue();
    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 11_000L);
    assertThat(book.onStreamFrame(10_020L, 10_980L)).isFalse();
    assertThat(book.isCovered()).isTrue();

    // the frame covering 11_000..11_999 was lost
    assertThat(book.onStreamFrame(12_010L, 12_990L)).isTrue();
    assertThat(book.isCovered()).isFalse();

    book.applySnapshot(List.of(dated("BTCUSDT", "100", 1_000L)), 13_100L);
    assertThat(book.isCovered()).isTrue();
  }

  private String price(String symbol) {
    return book.get(List.of(symbol)).get(0).getPrice().toString();
  }

  private static BigDecimalTicker dated(String symbol, String price, long time) {
    BigDecimalTicker ticker = new BigDecimalTicker();
    ticker.setSymbol(symbol);
    ticker.setPrice(new BigDecimal(price));
    ticker.setTime(time);
    return ticker;
  }
}
