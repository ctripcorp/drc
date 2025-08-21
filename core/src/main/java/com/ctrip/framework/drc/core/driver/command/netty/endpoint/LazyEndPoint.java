package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Created by dengquanliang
 * 2025/8/19 14:17
 */
public class LazyEndPoint implements Endpoint {

    private String ip;

    private int port;

    @JsonIgnore
    private String username;

    @JsonIgnore
    private String password;

    @JsonIgnore
    private String schema;

    @JsonIgnore
    private byte charsetNumber = 33;

    @JsonIgnore
    private final Supplier<InetSocketAddress> addressSupplier = () -> new InetSocketAddress(ip, port);

    public LazyEndPoint(String ip, int port) {
        this(ip, port, null, null);
    }

    public LazyEndPoint(String ip, int port, String username, String password) {
        this.ip = ip;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    @Override
    public String getScheme() {
        return schema;
    }

    @Override
    public String getHost() {
        return ip;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getUser() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return addressSupplier.get();
    }

    public String getAddress() {
        return ip + ":" + port;
    }

    public String getIp() {
        return ip;
    }

    @Override
    public String toString() {
        return "LazyEndPoint{" +
                "address=" + ip + ":" + port + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", schema='" + schema + '\'' +
                ", charsetNumber=" + charsetNumber +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LazyEndPoint that = (LazyEndPoint) o;
        return port == that.port && Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }
}
