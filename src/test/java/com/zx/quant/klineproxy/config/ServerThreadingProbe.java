package com.zx.quant.klineproxy.config;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Shared fixture for the two servlet-threading tests.
 *
 * <p>A minimal context rather than {@code KlineProxyApplication} on purpose: the question is about
 * the servlet container, and booting the WebSocket clients, schedulers and Prometheus registry
 * would only add ways to fail for unrelated reasons.
 *
 * @author flamhaze5946
 */
final class ServerThreadingProbe {

  private ServerThreadingProbe() {
  }

  static String call(int port) throws Exception {
    HttpResponse<String> response = HttpClient.newHttpClient()
        .send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/__thread_probe")).build(),
            HttpResponse.BodyHandlers.ofString());
    return response.body();
  }

  @RestController
  static class ProbeController {

    @GetMapping("/__thread_probe")
    String probe() {
      Thread t = Thread.currentThread();
      return t.isVirtual() + " " + t.getName();
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class MinimalServer {

    @Bean
    ProbeController probeController() {
      return new ProbeController();
    }
  }
}
