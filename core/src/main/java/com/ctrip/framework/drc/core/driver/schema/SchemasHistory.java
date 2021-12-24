package com.ctrip.framework.drc.core.driver.schema;

import com.ctrip.framework.drc.core.driver.binlog.gtid.GtidSet;
import com.ctrip.framework.drc.core.driver.schema.data.Columns;
import com.ctrip.framework.drc.core.driver.schema.data.Schemas;
import com.ctrip.framework.drc.core.driver.schema.data.TableKey;

/**
 * @Author Slight
 * Oct 11, 2019
 */
public interface SchemasHistory {
    void merge(GtidSet gtidExecuted, TableKey tableKey, Columns columns) throws Exception;
    boolean isMerged(GtidSet gtidExecuted, TableKey tableKey, Columns columns);
    Schemas query(String gtid);
}
