package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;

/**
 * @Author Slight
 * Jul 22, 2020
 */
public interface EventGroupContext extends GtidContext, GtidSetContext, DataIndexContext {

    default EventGroupContext begin(String gtid) {
        updateGtid(gtid);
        resetDataIndex();
        return this;
    }

    default void commit() {
        GtidSet set = fetchGtidSet();
        set.add(fetchGtid());
        updateGtidSet(set);
    }
}
