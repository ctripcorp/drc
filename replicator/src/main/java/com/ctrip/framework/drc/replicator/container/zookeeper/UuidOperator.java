package com.ctrip.framework.drc.replicator.container.zookeeper;

/**
 * @Author limingdong
 * @create 2022/4/2
 */
public interface UuidOperator {

    UuidConfig getUuids(String key);

    void setUuids(String key, UuidConfig value);
}
