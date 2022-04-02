package com.ctrip.framework.drc.core.driver.binlog.gtid;


import com.ctrip.framework.drc.core.server.observer.gtid.GtidObserver;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

import java.util.Set;

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

    boolean removeUuid(String uuid);
}
