package com.ctrip.framework.drc.core.driver.binlog.gtid;


import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.UUID;

/**
 * Created by mingdongli
 * 2019/9/17 下午8:20.
 */
public interface GtidManager extends Lifecycle, GtidReader, GtidObserver {

    void updateExecutedGtids(GtidSet gtidSet);

    void updatePurgedGtids(GtidSet gtidSet);

    boolean addExecutedGtid(String gtid);

    void setUuids(Set<String> uuids);

    Set<String> getUuids();

    void setCurrentUuid(String currentUuid);

    String getCurrentUuid();

    boolean removeUuid(String uuid);

    default Set<UUID> toUUIDSet() {
        Set<UUID> res = Sets.newHashSet();
        for (String uuid : getUuids()) {
            res.add(UUID.fromString(uuid));
        }
        return res;
    }

    default Set<String> toStringSet(Set<UUID> uuidSet) {
        Set<String> res = Sets.newHashSet();
        for (UUID uuid : uuidSet) {
            res.add(uuid.toString());
        }
        return res;
    }
}
