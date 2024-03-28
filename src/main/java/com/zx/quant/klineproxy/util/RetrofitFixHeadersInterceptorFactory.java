package com.zx.quant.klineproxy.util;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;
import org.apache.commons.collections4.MapUtils;
import org.jetbrains.annotations.NotNull;

/**
 * retrofit fix headers interceptor factory
 * @author flamhaze5946
 */
public class RetrofitFixHeadersInterceptorFactory {

  public static Interceptor createMockBrowserInterceptor() {
    return new FixHeadersInterceptor();
  }

  public static Interceptor createFixHeadersInterceptor(Map<String, String> headers) {
    return new FixHeadersInterceptor(headers);
  }

  /**
   * fix headers interceptor
   * @author flamhaze5946
   */
  private static class FixHeadersInterceptor implements Interceptor {

    private static final ImmutableMap<String, String> DEFAULT_MOCK_BROWSER_HEADERS = ImmutableMap.<String, String>builder()
        .put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0")
        .build();

    private final Map<String, String> fixHeaders;

    public FixHeadersInterceptor() {
      this(DEFAULT_MOCK_BROWSER_HEADERS);
    }

    private FixHeadersInterceptor(Map<String, String> fixHeaders) {
      this.fixHeaders = fixHeaders;
    }

    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
      Request originalRequest = chain.request();
      if (MapUtils.isEmpty(fixHeaders)) {
        return chain.proceed(originalRequest);
      }
      Builder builder = originalRequest.newBuilder();
      for (Entry<String, String> header : fixHeaders.entrySet()) {
        builder.header(header.getKey(), header.getValue());
      }
      Request withFixHeadersRequest = builder.build();
      return chain.proceed(withFixHeadersRequest);
    }
  }
}
