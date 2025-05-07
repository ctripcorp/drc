package com.ctrip.framework.drc.console.dto;

import java.util.List;

/**
 * Created by dengquanliang
 * 2024/12/10 14:54
 */
public class MhaColumnDefaultValueDto {

    private boolean querySuccess;
    private String mhaName;
    private List<TableColumnDto> tableColumns;

    public MhaColumnDefaultValueDto() {
    }

    public MhaColumnDefaultValueDto(String mhaName) {
        this.mhaName = mhaName;
        this.querySuccess = false;
    }

    public MhaColumnDefaultValueDto(String mhaName, List<TableColumnDto> tableColumns) {
        this.mhaName = mhaName;
        this.tableColumns = tableColumns;
        this.querySuccess = true;
    }

    public boolean isQuerySuccess() {
        return querySuccess;
    }

    public void setQuerySuccess(boolean querySuccess) {
        this.querySuccess = querySuccess;
    }

    public String getMhaName() {
        return mhaName;
    }

    public void setMhaName(String mhaName) {
        this.mhaName = mhaName;
    }

    public List<TableColumnDto> getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(List<TableColumnDto> tableColumns) {
        this.tableColumns = tableColumns;
    }

    @Override
    public String toString() {
        return "MhaColumnDefaultValueDto{" +
                "result=" + querySuccess +
                ", mhaName='" + mhaName + '\'' +
                ", tableColumns=" + tableColumns +
                '}';
    }

    public static class TableColumnDto {
        private String tableName;
        private List<ColumnDto> columns;

        public TableColumnDto(String tableName, List<ColumnDto> columns) {
            this.tableName = tableName;
            this.columns = columns;
        }

        public TableColumnDto() {
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public List<ColumnDto> getColumns() {
            return columns;
        }

        public void setColumns(List<ColumnDto> columns) {
            this.columns = columns;
        }

        @Override
        public String toString() {
            return "TableColumnDto{" +
                    "tableName='" + tableName + '\'' +
                    ", columns=" + columns +
                    '}';
        }
    }

    public static class ColumnDto {
        private String columnName;
        private String defaultValue;
        private int defaultValueLength;


        public ColumnDto(String columnName, String defaultValue, int defaultValueLength) {
            this.columnName = columnName;
            this.defaultValue = defaultValue;
            this.defaultValueLength = defaultValueLength;
        }

        public ColumnDto() {
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public int getDefaultValueLength() {
            return defaultValueLength;
        }

        public void setDefaultValueLength(int defaultValueLength) {
            this.defaultValueLength = defaultValueLength;
        }

        @Override
        public String toString() {
            return "ColumnDto{" +
                    "columnName='" + columnName + '\'' +
                    ", defaultValue='" + defaultValue + '\'' +
                    ", defaultValueLength=" + defaultValueLength +
                    '}';
        }
    }
}
