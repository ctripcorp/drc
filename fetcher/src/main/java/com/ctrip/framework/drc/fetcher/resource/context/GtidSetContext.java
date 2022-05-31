package com.ctrip.framework.drc.fetcher.resource.context;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;

/**
 * @Author Slight
 * Dec 03, 2019
 */
public interface GtidSetContext extends GtidContext {

    String KEY_NAME = "gtid set";

    default void updateGtidSet(GtidSet set) {
        update(KEY_NAME, set);
    }

    default GtidSet fetchGtidSet() {
        return ((GtidSet) fetch(KEY_NAME)).clone();
    }
}
