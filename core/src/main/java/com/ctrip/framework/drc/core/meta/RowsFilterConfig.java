package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class RowsFilterConfig {

    @JsonIgnore
    private String registryKey;

    private String mode;

    private String tables;

    private Fields parameters;

    private String expression;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables;
    }

    public Fields getParameters() {
        return parameters;
    }

    public void setParameters(Fields parameters) {
        this.parameters = parameters;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public boolean shouldFilterRows() {
        return RowsFilterType.None != getRowsFilterType();
    }

    public RowsFilterType getRowsFilterType() {
        return RowsFilterType.getType(mode);
    }

    public void setRegistryKey(String registryKey) {
        this.registryKey = registryKey;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    @Override
    public String toString() {
        return "RowsFilterConfig{" +
                "mode='" + mode + '\'' +
                ", tables='" + tables + '\'' +
                ", parameters=" + parameters +
                ", expression='" + expression + '\'' +
                '}';
    }

    public static class Fields {

        private List<String> columns;

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        @Override
        public String toString() {
            return "Fields{" +
                    "columns=" + columns +
                    '}';
        }
    }
}
