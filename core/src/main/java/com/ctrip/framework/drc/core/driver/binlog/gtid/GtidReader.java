package com.ctrip.framework.drc.core.driver.binlog.gtid;

import java.io.File;

/**
 * Created by mingdongli
 * 2019/10/25 下午11:42.
 */
public interface GtidReader {

    GtidSet getExecutedGtids();

    GtidSet getPurgedGtids();

    File getFirstLogNotInGtidSet(GtidSet gtidSet, boolean onlyLocalUuids);
}
