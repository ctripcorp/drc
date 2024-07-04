package com.ctrip.framework.drc.console.dto.v3;

import java.util.Objects;

public class LogicTableConfig {
    private String logicTable;
    private String dstLogicTable;
    private Long rowsFilterId;
    private Long colsFilterId;
    private Long messengerFilterId;

    public Long getMessengerFilterId() {
        return messengerFilterId;
    }

    public void setMessengerFilterId(Long messengerFilterId) {
        this.messengerFilterId = messengerFilterId;
    }

    public String getDstLogicTable() {
        return dstLogicTable;
    }

    public void setDstLogicTable(String dstLogicTable) {
        this.dstLogicTable = dstLogicTable;
    }

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
        LogicTableConfig that = (LogicTableConfig) o;
        return Objects.equals(logicTable, that.logicTable) && Objects.equals(dstLogicTable, that.dstLogicTable) && Objects.equals(rowsFilterId, that.rowsFilterId) && Objects.equals(colsFilterId, that.colsFilterId) && Objects.equals(messengerFilterId, that.messengerFilterId);
    }

    @Override
    public String toString() {
        return "LogicTableConfig{" +
                "logicTable='" + logicTable + '\'' +
                ", dstLogicTable='" + dstLogicTable + '\'' +
                ", rowsFilterId=" + rowsFilterId +
                ", colsFilterId=" + colsFilterId +
                ", messengerFilterId=" + messengerFilterId +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicTable, dstLogicTable, rowsFilterId, colsFilterId, messengerFilterId);
    }
}
