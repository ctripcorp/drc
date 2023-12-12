package com.ctrip.framework.drc.console.dto;

public class ProxyDto {

    private String dc;

    private String protocol;

    private String ip;

    private String port;

    public String getDc() {
        return dc;
    }

    public ProxyDto() {
    }

    public ProxyDto(String dc, String protocol, String ip, String port) {
        this.dc = dc;
        this.protocol = protocol;
        this.ip = ip;
        this.port = port;
    }

    public ProxyDto setDc(String dc) {
        this.dc = dc;
        return this;
    }

    public String getProtocol() {
        return protocol;
    }

    public ProxyDto setProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public String getIp() {
        return ip;
    }

    public ProxyDto setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public String getPort() {
        return port;
    }

    public ProxyDto setPort(String port) {
        this.port = port;
        return this;
    }

    @Override
    public String toString() {
        return "ProxyDto{" +
                "dc='" + dc + '\'' +
                ", protocol='" + protocol + '\'' +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                '}';
    }
}
