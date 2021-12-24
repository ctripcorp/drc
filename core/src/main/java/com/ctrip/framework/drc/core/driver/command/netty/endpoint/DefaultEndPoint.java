package com.ctrip.framework.drc.core.driver.command.netty.endpoint;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * Created by mingdongli
 * 2019/9/6 上午11:25.
 */
public class DefaultEndPoint implements Endpoint {

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
    private InetSocketAddress address;

    public DefaultEndPoint() {
    }

    public DefaultEndPoint(String ip, int port) {
        this(ip, port, null, null);
    }

    public DefaultEndPoint(String ip, int port, String username, String password) {
        this.ip = ip;
        this.port = port;
        this.username = username;
        this.password = password;
        this.address = new InetSocketAddress(ip, port);
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
        return address;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "DefaultEndPoint{" +
                "address=" + address +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", schema='" + schema + '\'' +
                ", charsetNumber=" + charsetNumber +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultEndPoint that = (DefaultEndPoint) o;
        return Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {

        return Objects.hash(address);
    }
}
