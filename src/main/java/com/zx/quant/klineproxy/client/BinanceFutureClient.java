package com.zx.quant.klineproxy.client;

import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceServerTime;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.FuturePremiumIndex;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import java.util.List;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface BinanceFutureClient {

  @GET("fapi/v1/time")
  Call<BinanceServerTime> getServerTime();

  @GET("fapi/v1/exchangeInfo")
  Call<BinanceFutureExchange> getExchange();

  @GET("fapi/v1/fundingRate")
  Call<List<FutureFundingRate>> getFundingRates(
      @Query("limit") Integer limit
  );

  @GET("fapi/v1/premiumIndex")
  Call<List<FuturePremiumIndex>> getSymbolPremiumIndices();

  @GET("fapi/v1/klines")
  Call<List<Object[]>> getKlines(
      @Query("symbol") String symbol,
      @Query("interval") String interval,
      @Query("startTime") Long startTime,
      @Query("endTime") Long endTime,
      @Query("limit") Integer limit
  );

  @GET("fapi/v1/ticker/24hr")
  Call<List<Ticker24Hr>> getTicker24hr();
}
