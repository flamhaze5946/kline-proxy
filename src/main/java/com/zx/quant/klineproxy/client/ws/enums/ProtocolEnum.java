package com.zx.quant.klineproxy.client.ws.enums;

import com.zx.quant.klineproxy.util.BaseEnum;

/**
 * protocol enum
 * @author flamhaze5946
 */
public enum ProtocolEnum implements BaseEnum {

  WS("ws", 80, false, "ws protocol"),

  WSS("wss", 443, true, "ws protocol"),
  ;

  private final String code;

  private final int port;

  private final boolean ssl;

  private final String description;

  ProtocolEnum(String code, int port, boolean ssl, String description) {
    this.code = code;
    this.port = port;
    this.ssl = ssl;
    this.description = description;
  }

  public int getPort() {
    return port;
  }

  public boolean isSsl() {
    return ssl;
  }

  @Override
  public String code() {
    return code;
  }

  @Override
  public String description() {
    return description;
  }
}
