package com.ctrip.framework.drc.replicator.container.zookeeper;

import java.util.Set;

/**
 * Created by mingdongli
 * 2019/12/10 下午7:56.
 */
public class UuidConfig {

    private Set<String> uuids;

    public UuidConfig(Set<String> uuids) {
        this.uuids = uuids;
    }

    public Set<String> getUuids() {
        return uuids;
    }

    public void setUuids(Set<String> uuids) {
        this.uuids = uuids;
    }

}
