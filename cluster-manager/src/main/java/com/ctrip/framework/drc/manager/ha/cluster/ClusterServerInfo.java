package com.ctrip.framework.drc.manager.ha.cluster;

import com.ctrip.framework.drc.manager.enums.ServerStateEnum;
import com.ctrip.xpipe.api.codec.Codec;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Objects;

/**
 * @Author limingdong
 * @create 2020/4/19
 */
public class ClusterServerInfo {
    private String ip;
    private int port;
    /**
     * @see ServerStateEnum
     */
    private int state;

    public ClusterServerInfo() {

    }

    public ClusterServerInfo(String ip, int port) {
        this(ip, port, ServerStateEnum.NORMAL);
    }

    public ClusterServerInfo(String ip, int port, ServerStateEnum stateEnum) {
        this.ip = ip;
        this.port = port;
        this.state = stateEnum.getCode();
    }

    public String getIp() {
        return ip;
    }


    public int getPort() {
        return port;
    }


    public void setPort(int port) {
        this.port = port;
    }

    public int getState() {
        return state;
    }

    public void setState(ServerStateEnum state) {
        this.state = state.getCode();
    }

    @JsonIgnore
    public ServerStateEnum getStateEnum() {
        return ServerStateEnum.parseCode(state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterServerInfo)) return false;
        ClusterServerInfo that = (ClusterServerInfo) o;
        return port == that.port && state == that.state && Objects.equals(ip, that.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port, state);
    }

    @Override
    public String toString() {
        return Codec.DEFAULT.encode(this);
    }
}
