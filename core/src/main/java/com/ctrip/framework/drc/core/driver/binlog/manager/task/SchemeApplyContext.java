package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.driver.binlog.constant.QueryType;

/**
 * @Author limingdong
 * @create 2022/11/23
 */
public class SchemeApplyContext {

    private String schema;

    private String table;

    private String ddl;

    private String gtid;

    private QueryType queryType;

    private String registryKey;

    private SchemeApplyContext(String schema, String table, String ddl, String gtid, QueryType queryType, String registryKey) {
        this.schema = schema;
        this.table = table;
        this.ddl = ddl;
        this.gtid = gtid;
        this.queryType = queryType;
        this.registryKey = registryKey;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getDdl() {
        return ddl;
    }

    public String getGtid() {
        return gtid;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public String getRegistryKey() {
        return registryKey;
    }

    public static class Builder {

        private String schema;

        private String table;

        private String ddl;

        private String gtid;

        private QueryType queryType;

        private String registryKey;

        public Builder schema(String schema) {
            this.schema = schema;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder ddl(String ddl) {
            this.ddl = ddl;
            return this;
        }

        public Builder gtid(String gtid) {
            this.gtid = gtid;
            return this;
        }

        public Builder registryKey(String registryKey) {
            this.registryKey = registryKey;
            return this;
        }

        public Builder queryType(QueryType queryType) {
            this.queryType = queryType;
            return this;
        }

        public SchemeApplyContext build() {
            return new SchemeApplyContext(schema, table, ddl, gtid, queryType, registryKey);
        }

    }
}
