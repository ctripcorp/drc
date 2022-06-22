package com.ctrip.framework.drc.console.vo;

import com.ctrip.framework.drc.console.utils.MySqlUtils;

/**
 * @ClassName TableCheckVo
 * @Author haodongPan
 * @Date 2022/6/17 14:42
 * @Version: $
 */
public class TableCheckVo {
    
    private String schema;
    private String table;
    private boolean noOnUpdateColumn;
    private boolean noOnUpdateKey;
    private boolean noPkUk;
    private boolean approveTruncate;

    public String getFullName() {
        return String.format("`%s`.`%s`", schema, table);
    }
    
    public TableCheckVo(MySqlUtils.TableSchemaName tableSchemaName) {
        this.schema = tableSchemaName.getSchema();
        this.table = tableSchemaName.getName();
    }
    
    @Override
    public String toString() {
        return "TableCheckVo{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", noOnUpdateColumn=" + noOnUpdateColumn +
                ", noOnUpdateKey=" + noOnUpdateKey +
                ", noPkUk=" + noPkUk +
                ", approveTruncate=" + approveTruncate +
                '}';
    }
    
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

    public boolean getNoOnUpdateColumn() {
        return noOnUpdateColumn;
    }

    public void setNoOnUpdateColumn(boolean noOnUpdateColumn) {
        this.noOnUpdateColumn = noOnUpdateColumn;
    }

    public boolean getNoOnUpdateKey() {
        return noOnUpdateKey;
    }

    public void setNoOnUpdateKey(boolean noOnUpdateKey) {
        this.noOnUpdateKey = noOnUpdateKey;
    }

    public boolean getNoPkUk() {
        return noPkUk;
    }

    public void setNoPkUk(boolean noPkUk) {
        this.noPkUk = noPkUk;
    }

    public boolean getApproveTruncate() {
        return approveTruncate;
    }

    public void setApproveTruncate(boolean approveTruncate) {
        this.approveTruncate = approveTruncate;
    }
}
