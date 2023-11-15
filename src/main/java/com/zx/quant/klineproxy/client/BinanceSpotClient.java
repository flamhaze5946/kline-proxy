package com.zx.quant.klineproxy.client;

import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import java.util.List;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface BinanceSpotClient {

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
}
