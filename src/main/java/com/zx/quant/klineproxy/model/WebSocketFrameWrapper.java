package com.zx.quant.klineproxy.model;

import io.netty.handler.codec.http.websocketx.WebSocketFrame;

/**
 * websocket frame wrapper
 * @author flamhaze5946
 */
public record WebSocketFrameWrapper(WebSocketFrame frame, Runnable afterSendFunc) {
}
