package com.zx.quant.klineproxy.config;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * The unit tests all run through MockMvc, which never touches the servlet container — so they
 * cannot tell whether request dispatch actually moved onto virtual threads. This boots a real
 * server and asks the handler thread itself.
 *
 * <p>This is the test that decided the container swap: on Undertow the same probe kept reporting
 * {@code XNIO-N task-M} with every hook tried ({@code DeploymentInfo.setExecutor},
 * {@code addInitialHandlerChainWrapper}), because Spring Boot's virtual-thread support covers
 * Tomcat and Jetty only. Measured 2026-09-09.
 *
 * @author flamhaze5946
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = ServerThreadingProbe.MinimalServer.class,
    properties = "spring.threads.virtual.enabled=true")
class VirtualThreadsEnabledTest {

  @LocalServerPort
  int port;

  @Test
  void dispatches_requests_on_virtual_threads() throws Exception {
    String body = ServerThreadingProbe.call(port);
    assertTrue(body.startsWith("true "), "handler thread was not virtual: " + body);
  }
}
