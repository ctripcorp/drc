package com.ctrip.framework.drc.console.monitor.delay.config;

import java.util.Objects;

/**
 * Created by mingdongli
 * 2019/12/19 下午3:39.
 */
public class DelayMonitorConfig {

    private String schema;

    private String table;

    private String key;

    private String onUpdate;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getOnUpdate() {
        return onUpdate;
    }

    public void setOnUpdate(String onUpdate) {
        this.onUpdate = onUpdate;
    }

    public String getTableSchema() {
        return getSchema() + "." + getTable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelayMonitorConfig that = (DelayMonitorConfig) o;
        return Objects.equals(schema, that.schema) &&
                Objects.equals(table, that.table) &&
                Objects.equals(key, that.key) &&
                Objects.equals(onUpdate, that.onUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, table, key, onUpdate);
    }
}
