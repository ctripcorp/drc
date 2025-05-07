package com.ctrip.framework.drc.console.enums;

/**
 * Created by dengquanliang
 * 2025/4/1 17:16
 */
public enum ProxyEnum {

    PROXY("PROXY", 80),
    PROXYTLS("PROXYTLS", 443);

    private String protocol;
    private int port;

    ProxyEnum(String protocol, int port) {
        this.protocol = protocol;
        this.port = port;
    }

    public String getProtocol() {
        return protocol;
    }

    public int getPort() {
        return port;
    }
}
