package com.ctrip.framework.drc.console.dto;

import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.utils.DalUtils;
import org.apache.commons.lang3.StringUtils;

public class ProxyDto {

    private DalUtils dalUtils = DalUtils.getInstance();

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

    public ProxyTbl toProxyTbl() throws Exception {

        if(!prerequisite()) {
            throw new IllegalArgumentException("all args should be not null: " + this);
        }

        ProxyTbl proxyTbl = new ProxyTbl();
        Long dcId = dalUtils.updateOrCreateDc(this.dc);
        proxyTbl.setDcId(dcId);
        proxyTbl.setUri(String.format("%s://%s:%s", this.protocol, this.ip, this.port));
        proxyTbl.setActive(BooleanEnum.TRUE.getCode());
        proxyTbl.setMonitorActive(BooleanEnum.FALSE.getCode());
        proxyTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return proxyTbl;
    }

    private boolean prerequisite() {
        return StringUtils.isNotBlank(this.dc)
                && StringUtils.isNotBlank(this.protocol)
                && StringUtils.isNotBlank(this.ip)
                && StringUtils.isNotBlank(this.port);
    }
}
