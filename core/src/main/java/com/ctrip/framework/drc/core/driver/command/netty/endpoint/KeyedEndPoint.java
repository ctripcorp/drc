package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Created by yongnian
 * 2024/01/06 16:25.
 */
public class KeyedEndPoint implements Endpoint {
    private final String key;
    private final DefaultEndPoint endPoint;

    public KeyedEndPoint(String key, DefaultEndPoint endPoint) {
        this.key = key;
        this.endPoint = endPoint;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String getScheme() {
        return endPoint.getScheme();
    }

    @Override
    public String getHost() {
        return endPoint.getHost();
    }

    @Override
    public int getPort() {
        return endPoint.getPort();
    }

    @Override
    public String getUser() {
        return endPoint.getUser();
    }

    @Override
    public String getPassword() {
        return endPoint.getPassword();
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return endPoint.getSocketAddress();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyedEndPoint)) return false;
        KeyedEndPoint that = (KeyedEndPoint) o;
        return Objects.equals(key, that.key) && Objects.equals(endPoint, that.endPoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, endPoint);
    }
}
