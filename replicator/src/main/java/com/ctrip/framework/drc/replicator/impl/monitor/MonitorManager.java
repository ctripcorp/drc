package com.ctrip.framework.drc.replicator.impl.monitor;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.ctrip.framework.drc.core.driver.binlog.impl.UpdateRowsEvent;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-12-30
 */
public interface MonitorManager {

    void onTableMapLogEvent(TableMapLogEvent tableMapLogEvent);

    boolean onUpdateRowsEvent(UpdateRowsEvent updateRowsEvent, String gtid);

    void onDdlEvent(String schema, String tableName, String ddl, QueryType queryType);
}
