package com.ctrip.framework.drc.core.driver.schema.data;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import java.util.List;

/**
 * @Author Slight
 * Oct 12, 2019
 */
public class TableKey extends Key {

    public final String databaseName;

    public final String tableName;

    public List<TableMapLogEvent.Column> columns;

    public TableKey(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public static TableKey from(String databaseName, String tableName) {
        return new TableKey(databaseName, tableName);
    }

    public static TableKey from(String tableKeyString) {
        String[] dat = tableKeyString.replace("`", "").split("\\.");
        String databaseName = dat[0];
        String tableName = dat[1];
        return TableKey.from(databaseName, tableName);
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<TableMapLogEvent.Column> getColumns() {
        return columns;
    }

    public void setColumns(List<TableMapLogEvent.Column> columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "`" + databaseName.toLowerCase()/*temp code 2020.7.12*/ + "`.`" + tableName + "`";
    }

    @Override
    public TableKey clone() {
        return TableKey.from(databaseName, tableName);
    }
}
