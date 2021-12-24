package com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy;

import io.netty.buffer.ByteBuf;

/**
 * @Author limingdong
 * @create 2021/4/9
 */
public interface ProxyProtocol {
    String PLUS = "+";

    String KEY_WORD = "PROXY";

    String ROUTE = "ROUTE";

    ByteBuf output();

}
