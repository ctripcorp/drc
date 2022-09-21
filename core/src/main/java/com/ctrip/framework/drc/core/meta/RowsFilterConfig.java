package com.ctrip.framework.drc.core.meta;

import com.ctrip.framework.drc.core.server.common.enums.RowsFilterType;
import com.ctrip.framework.drc.core.server.common.filter.row.FetchMode;
import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterRule;
import com.ctrip.framework.drc.core.server.common.filter.row.UserFilterMode;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.Objects;

/**
 * @Author limingdong
 * @create 2022/4/27
 */
public class RowsFilterConfig {

    @JsonIgnore
    private String registryKey;

    private String mode;

    private String tables;

    @Deprecated
    private Parameters parameters;

    private Configs configs;

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
        return getConfigs().getParameters().get(0);
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

    public Configs getConfigs() {
        return configs;
    }

    public void setConfigs(Configs configs) {
        this.configs = configs;
    }

    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
    }

    public Class<? extends RowsFilterRule> getRowsFilterRule() throws ClassNotFoundException {
        RowsFilterType rowFilterType = getRowsFilterType();
        return rowFilterType.filterRuleClass();
    }

    @Override
    public String toString() {
        return "RowsFilterConfig{" +
                "registryKey='" + registryKey + '\'' +
                ", mode='" + mode + '\'' +
                ", tables='" + tables + '\'' +
                ", parameters=" + parameters +
                ", configs=" + configs +
                '}';
    }

    public static class Parameters implements Comparable<Parameters> {

        private List<String> columns;

        private boolean illegalArgument;

        private String context;

        private int fetchMode = FetchMode.RPC.getCode();

        private String userFilterMode;

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

        public int getFetchMode() {
            return fetchMode;
        }

        public void setFetchMode(int fetchMode) {
            this.fetchMode = fetchMode;
        }

        public String getUserFilterMode() {
            return userFilterMode;
        }

        public void setUserFilterMode(String userFilterMode) {
            this.userFilterMode = userFilterMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Parameters)) return false;
            Parameters that = (Parameters) o;
            return illegalArgument == that.illegalArgument &&
                    fetchMode == that.fetchMode &&
                    Objects.equals(columns, that.columns) &&
                    Objects.equals(context, that.context) &&
                    Objects.equals(userFilterMode, that.userFilterMode);
        }

        @Override
        public int hashCode() {

            return Objects.hash(columns, illegalArgument, context, fetchMode, userFilterMode);
        }

        @Override
        public String toString() {
            return "Parameters{" +
                    "columns=" + columns +
                    ", illegalArgument=" + illegalArgument +
                    ", context='" + context + '\'' +
                    ", fetchMode=" + fetchMode +
                    ", userFilterMode='" + userFilterMode + '\'' +
                    '}';
        }

        @Override
        public int compareTo(Parameters o) {
            UserFilterMode userFilterMode = UserFilterMode.getUserFilterMode(getUserFilterMode());
            UserFilterMode thatUserFilterMode = UserFilterMode.getUserFilterMode(o.getUserFilterMode());
            return userFilterMode.getPriority() - thatUserFilterMode.getPriority();
        }
    }

    public static class Configs {

        private List<Parameters> parameters;

        public List<Parameters> getParameters() {
            return parameters;
        }

        public void setParameters(List<Parameters> parameters) {
            this.parameters = parameters;
        }

        @Override
        public String toString() {
            return "Configs{" +
                    "parameters=" + parameters +
                    '}';
        }
    }
}
