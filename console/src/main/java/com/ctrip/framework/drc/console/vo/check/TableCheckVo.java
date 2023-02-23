package com.ctrip.framework.drc.console.vo.check;

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
    private boolean noStandardOnUpdateColumn;
    private boolean noOnUpdateKey;
    private boolean noPkUk;
    private boolean approveTruncate;
    private boolean timeDefaultZero;
    private String res;

    public String getFullName() {
        return String.format("`%s`.`%s`", schema, table);
    }
    
    public TableCheckVo(MySqlUtils.TableSchemaName tableSchemaName) {
        this.schema = tableSchemaName.getSchema();
        this.table = tableSchemaName.getName();
    }

    public boolean hasProblem() {
        if (approveTruncate) {
            res = "approveTruncate";
            return true;
        }
        if (noPkUk) {
            res = "noPkUk";
            return true;
        }
        if (noOnUpdateColumn) {
            res = "noOnUpdateColumn";
            return true;
        }
        if (timeDefaultZero) {
            res = "timeDefaultZero";
            return true;
        }
        if (noStandardOnUpdateColumn) {
            res = "noStandardOnUpdateColumn";
            return true;
        }
        res = "ok";
        return false;
    }

    @Override
    public String toString() {
        return "TableCheckVo{" +
                "schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", noOnUpdateColumn=" + noOnUpdateColumn +
                ", noStandardOnUpdateColumn=" + noStandardOnUpdateColumn +
                ", noOnUpdateKey=" + noOnUpdateKey +
                ", noPkUk=" + noPkUk +
                ", approveTruncate=" + approveTruncate +
                ", timeDefaultZero=" + timeDefaultZero +
                ", res='" + res + '\'' +
                '}';
    }

    public boolean getTimeDefaultZero() {
        return timeDefaultZero;
    }

    public void setTimeDefaultZero(boolean timeDefaultZero) {
        this.timeDefaultZero = timeDefaultZero;
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

    public boolean getNoStandardOnUpdateColumn() {
        return noStandardOnUpdateColumn;
    }

    public void setNoStandardOnUpdateColumn(boolean noStandardOnUpdateColumn) {
        this.noStandardOnUpdateColumn = noStandardOnUpdateColumn;
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

    public String getRes() {
        return res;
    }

    public void setRes(String res) {
        this.res = res;
    }
}
