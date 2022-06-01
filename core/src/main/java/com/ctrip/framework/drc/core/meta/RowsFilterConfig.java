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

    private Parameters parameters;

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

    public Parameters getParameters() {
        return parameters;
    }

    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
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
                "registryKey='" + registryKey + '\'' +
                ", mode='" + mode + '\'' +
                ", tables='" + tables + '\'' +
                ", parameters=" + parameters +
                '}';
    }

    public static class Parameters {

        private List<String> columns;

        private boolean illegalArgument;

        private String context;

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        public String getContext() {
            return context;
        }

        public void setContext(String context) {
            this.context = context;
        }

        public boolean getIllegalArgument() {
            return illegalArgument;
        }

        public void setIllegalArgument(boolean illegalArgument) {
            this.illegalArgument = illegalArgument;
        }

        @Override
        public String toString() {
            return "Parameters{" +
                    "columns=" + columns +
                    ", illegalArgument=" + illegalArgument +
                    ", context='" + context + '\'' +
                    '}';
        }
    }
}
