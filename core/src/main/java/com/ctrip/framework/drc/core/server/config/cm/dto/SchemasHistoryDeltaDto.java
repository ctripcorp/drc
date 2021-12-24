package com.ctrip.framework.drc.core.server.config.cm.dto;

import com.ctrip.framework.drc.core.driver.binlog.impl.TableMapLogEvent;

import java.util.ArrayList;
import java.util.Objects;

/**
 * @Author Slight
 * Nov 19, 2019
 */
public class SchemasHistoryDeltaDto {

    private String gtidExecuted;
    private String table;
    private ArrayList<TableMapLogEvent.Column> columns;

    public ArrayList<TableMapLogEvent.Column> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<TableMapLogEvent.Column> columns) {
        this.columns = columns;
    }

    public String getGtidExecuted() {
        return gtidExecuted;
    }

    public void setGtidExecuted(String gtidExecuted) {
        this.gtidExecuted = gtidExecuted;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemasHistoryDeltaDto that = (SchemasHistoryDeltaDto) o;
        return Objects.equals(gtidExecuted, that.gtidExecuted) &&
                Objects.equals(table, that.table) &&
                Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gtidExecuted, table, columns);
    }

    @Override
    public String toString() {
        return "SchemeHistoryDeltaDto{" +
                "gtidExecuted='" + gtidExecuted + '\'' +
                ", table='" + table + '\'' +
                ", columns=" + columns +
                '}';
    }
}
