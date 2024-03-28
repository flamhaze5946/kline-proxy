package com.zx.quant.klineproxy.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.client.BinanceCompositeClient;
import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.client.BinanceSpotClient;
import com.zx.quant.klineproxy.util.RetrofitFixHeadersInterceptorFactory;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/**
 * client config
 * @author flamhaze5946
 */
@Configuration
public class ClientConfig {

  @Value("${client.binanceSpot.api.rootUrl:https://api.binance.com}")
  private String binanceSpotApiRootUrl;

  @Value("${client.binanceFuture.api.rootUrl:https://fapi.binance.com}")
  private String binanceFutureApiRootUrl;

  @Value("${client.binanceComposite.api.rootUrl:https://www.binance.com}")
  private String binanceCompositeRootUrl;

  /**
   * create binance spot clients
   * @return binance spot clients
   */
  @Bean
  public BinanceSpotClient binanceSpotClient() {
    Retrofit retrofit = new Retrofit.Builder()
        .addConverterFactory(JacksonConverterFactory.create())
        .baseUrl(binanceSpotApiRootUrl)
        .build();

    return retrofit.create(BinanceSpotClient.class);
  }

  /**
   * create binance future clients
   * @return binance future clients
   */
  @Bean
  public BinanceFutureClient binanceFutureClient() {
    Retrofit retrofit = new Retrofit.Builder()
        .addConverterFactory(JacksonConverterFactory.create())
        .baseUrl(binanceFutureApiRootUrl)
        .build();

    return retrofit.create(BinanceFutureClient.class);
  }

  @Bean
  public BinanceCompositeClient binanceCompositeClient(ObjectMapper objectMapper) {
    OkHttpClient client = new OkHttpClient().newBuilder()
        .addInterceptor(RetrofitFixHeadersInterceptorFactory.createMockBrowserInterceptor())
        .build();

    Retrofit retrofit = new Retrofit.Builder()
        .addConverterFactory(JacksonConverterFactory.create(objectMapper))
        .baseUrl(binanceCompositeRootUrl)
        .client(client)
        .build();

    return retrofit.create(BinanceCompositeClient.class);
  }
}
