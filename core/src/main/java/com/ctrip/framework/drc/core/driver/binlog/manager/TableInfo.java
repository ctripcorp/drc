package com.ctrip.framework.drc.core.driver.binlog.manager;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/9/29 下午2:27.
 */
public class TableInfo {

    private String dbName;

    private String tableName;

    private List<TableMapLogEvent.Column> columnList = Lists.newArrayList();

    private List<List<String>> identifiers = Lists.newArrayList();

    public TableInfo() {
    }

    public TableInfo(String dbName, String tableName, List<TableMapLogEvent.Column> columnList, List<List<String>> identifiers) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnList = columnList;
        this.identifiers = identifiers;
    }

    public TableMapLogEvent.Column getFieldMetaByName(String name) {
        for (TableMapLogEvent.Column meta : columnList) {
            if (meta.getName().equalsIgnoreCase(name)) {
                return meta;
            }
        }

        return null;
    }

    public void addFieldMeta(TableMapLogEvent.Column fieldMeta) {
        this.columnList.add(fieldMeta);
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<TableMapLogEvent.Column> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<TableMapLogEvent.Column> columnList) {
        this.columnList = columnList;
    }

    public void addColumn(TableMapLogEvent.Column column) {
        columnList.add(column);
    }

    public List<List<String>> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(List<List<String>> identifiers) {
        this.identifiers = identifiers;
    }
}
