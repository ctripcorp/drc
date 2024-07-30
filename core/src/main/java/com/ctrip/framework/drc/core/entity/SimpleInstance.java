package com.ctrip.framework.drc.core.entity;

import java.util.Objects;

/**
 * @author: yongnian
 * @create: 2024/5/22 20:04
 */
public class SimpleInstance implements Instance {

    private String ip;
    private Integer port;
    private boolean master = false;


    public static SimpleInstance from(String ip, Integer port) {
        SimpleInstance instance = new SimpleInstance();
        instance.ip = ip;
        instance.port = port;
        return instance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimpleInstance)) return false;
        SimpleInstance instance = (SimpleInstance) o;
        return master == instance.master && Objects.equals(ip, instance.ip) && Objects.equals(port, instance.port);
    }

    @Override
    public String toString() {
        return String.format("{ip='%s', port=%d, master=%s}", ip, port, master);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port, master);
    }

    @Override
    public String getIp() {
        return ip;
    }

    @Override
    public Integer getPort() {
        return port;
    }

    @Override
    public boolean getMaster() {
        return master;
    }

    @Override
    public Instance setIp(String ip) {
        this.ip = ip;
        return this;
    }

    @Override
    public Instance setPort(Integer port) {
        this.port = port;
        return this;
    }

    @Override
    public Instance setMaster(boolean master) {
        this.master = master;
        return this;
    }
}
