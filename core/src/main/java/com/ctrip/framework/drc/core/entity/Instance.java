package com.ctrip.framework.drc.core.entity;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @Author limingdong
 * @create 2020/4/29
 */
public interface Instance {

    String getIp();

    Integer getPort();

    boolean getMaster();

    Instance setIp(String ip);

    Instance setPort(Integer port);

    Instance setMaster(boolean master);

    default boolean equalsWithIpPort(HostPort hostPort) {

        if (!ObjectUtils.equals(getIp(), hostPort.getHost())) {
            return false;
        }

        if (!ObjectUtils.equals(getPort(), hostPort.getPort())) {
            return false;
        }
        return true;
    }

    default boolean equalsWithIpPort(Instance instance) {

        if (instance == null) {
            return false;
        }

        if (!ObjectUtils.equals(getIp(), instance.getIp())) {
            return false;
        }

        if (!ObjectUtils.equals(getPort(), instance.getPort())) {
            return false;
        }

        return true;
    }

    default String desc() {
        return String.format("%s(%s:%d)", getClass().getSimpleName(), getIp(), getPort());
    }
}
