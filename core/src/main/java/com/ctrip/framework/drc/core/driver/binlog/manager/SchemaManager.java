package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.driver.ConnectionObserver;
import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;
import com.ctrip.framework.drc.core.driver.binlog.impl.DrcSchemaSnapshotLogEvent;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.List;
import java.util.Map;

/**
 * Created by mingdongli
 * 2019/9/21 下午9:22
 */
public interface SchemaManager extends Lifecycle, ConnectionObserver {

    TableInfo find(String schema, String table);

    ApplyResult apply(String schema, String table, String ddl, QueryType queryType, String gtid);

    /**
     * for recovery
     * @return
     */
    Map<String/* schema */, Map<String/* table */, String>> snapshot();

    /**
     * should call it after update schema
     * @param tableIds related tables
     */
    void refresh(List<TableId> tableIds);
    /**
     * recover embedded db from snapshot
     *
     * @param snapshotLogEvent event
     * @return
     */
    boolean recovery(DrcSchemaSnapshotLogEvent snapshotLogEvent, boolean fromLatestLocalBinlog);
    /**
     * will recover from event or not
     * @param fromLatestLocalBinlog true if event from local binlog instead of other replicators
     */
    boolean shouldRecover(boolean fromLatestLocalBinlog);


    boolean clone(Endpoint endpoint);

    TableInfo queryTableInfoByIS(DataSource dataSource, String schema, String table);

    void persistColumnInfo(TableInfo tableInfo, boolean writeDirect);

    void persistDdl(String dbName, String tableName, String queryString);

}
