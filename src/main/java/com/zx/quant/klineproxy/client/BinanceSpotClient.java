package com.zx.quant.klineproxy.client;

import com.zx.quant.klineproxy.client.model.BinanceServerTime;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import java.util.List;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface BinanceSpotClient {

  @GET("api/v3/time")
  Call<BinanceServerTime> getServerTime();

  @GET("api/v3/exchangeInfo")
  Call<BinanceSpotExchange> getExchange();

  @GET("api/v3/klines")
  Call<List<Object[]>> getKlines(
      @Query("symbol") String symbol,
      @Query("interval") String interval,
      @Query("startTime") Long startTime,
      @Query("endTime") Long endTime,
      @Query("limit") Integer limit
  );

  @GET("api/v3/klines")
  Call<List<Object[]>> getKlines(
      @Query("symbol") String symbol,
      @Query("interval") String interval,
      @Query("startTime") Long startTime,
      @Query("endTime") Long endTime,
      @Query("limit") Integer limit,
      @Query("timeZone") String timeZone
  );

  @GET("api/v3/ticker/24hr")
  Call<List<Ticker24Hr>> getTicker24hr();

  @GET("api/v3/ticker/24hr")
  Call<Ticker24Hr> getSymbolTicker24hr(
      @Query("symbol") String symbol
  );

  @GET("api/v3/ticker/24hr")
  Call<List<Ticker24Hr>> getSymbolsTicker24hr(
      @Query("symbols") String symbols
  );

  @GET("api/v3/ticker/price")
  Call<List<Ticker.BigDecimalTicker>> getTickerPrices();

  @GET("api/v3/ticker/price")
  Call<Ticker.BigDecimalTicker> getSymbolTickerPrice(
      @Query("symbol") String symbol
  );

  @GET("api/v3/ticker/price")
  Call<List<Ticker.BigDecimalTicker>> getSymbolsTickerPrice(
      @Query("symbols") String symbols
  );
}
