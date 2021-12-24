package com.ctrip.framework.drc.core.driver.command.netty.endpoint.proxy;

import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @Author limingdong
 * @create 2021/4/9
 */
public class ProxyEnabledEndpoint extends DefaultEndPoint implements Endpoint, ProxyEnabled {

    @JsonIgnore
    private ProxyProtocol proxyProtocol;

    public ProxyEnabledEndpoint(String ip, int port, ProxyProtocol proxyProtocol) {
        super(ip, port);
        this.proxyProtocol = proxyProtocol;
    }

    @Override
    @JsonIgnore
    public ProxyProtocol getProxyProtocol() {
        return proxyProtocol;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}