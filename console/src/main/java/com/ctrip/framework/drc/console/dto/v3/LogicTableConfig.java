package com.ctrip.framework.drc.console.dto.v3;

import java.util.Objects;

public class LogicTableConfig {
    private String logicTable;
    private Long rowsFilterId;
    private Long colsFilterId;

    public String getLogicTable() {
        return logicTable;
    }

    public void setLogicTable(String logicTable) {
        this.logicTable = logicTable;
    }

    public Long getRowsFilterId() {
        return rowsFilterId;
    }

    public void setRowsFilterId(Long rowsFilterId) {
        this.rowsFilterId = rowsFilterId;
    }

    public Long getColsFilterId() {
        return colsFilterId;
    }

    public void setColsFilterId(Long colsFilterId) {
        this.colsFilterId = colsFilterId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogicTableConfig)) return false;
        LogicTableConfig config = (LogicTableConfig) o;
        return Objects.equals(logicTable, config.logicTable) && Objects.equals(rowsFilterId, config.rowsFilterId) && Objects.equals(colsFilterId, config.colsFilterId);
    }

    @Override
    public String toString() {
        return "LogicTableConfig{" +
                "logicTable='" + logicTable + '\'' +
                ", rowsFilterId=" + rowsFilterId +
                ", colsFilterId=" + colsFilterId +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicTable, rowsFilterId, colsFilterId);
    }
}
