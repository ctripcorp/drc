package com.ctrip.framework.drc.fetcher.event;

/**
 * @Author Slight
 * Oct 16, 2019
 */
public class MockTableMapEvent extends MonitoredTableMapEvent {

    public String schemaName;
    public String tableName;

    public MockTableMapEvent(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getSchemaNameDotTableName() {
        return schemaName + "." + tableName;
    }

}
