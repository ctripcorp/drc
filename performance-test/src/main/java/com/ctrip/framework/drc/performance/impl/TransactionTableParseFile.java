package com.ctrip.framework.drc.performance.impl;

import com.ctrip.framework.drc.applier.activity.replicator.converter.TransactionTableApplierByteBufConverter;
import com.ctrip.framework.drc.core.monitor.datasource.DataSourceManager;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by jixinwang on 2021/8/27
 */
public class TransactionTableParseFile extends ParseFile {

    private static String CREATE_MONITOR_DATABASE = "create database if not exists drcmonitordb;";

    private static String CREATE_GTID_EXECUTED_TABLE = "CREATE TABLE IF NOT EXISTS drcmonitordb.gtid_executed (\n" +
            "    id int(11) NOT NULL,\n" +
            "    server_uuid char(36) NOT NULL,\n" +
            "    gno bigint(20) NOT NULL,\n" +
            "    gtidset longtext,\n" +
            "  primary key ix_gtid (id, server_uuid)\n" +
            ");";

    public TransactionTableParseFile(Endpoint endPoint) {
        super(endPoint);
    }

    @Override
    protected void doInitialize() {
        super.doInitialize();
        byteBufConverter = new TransactionTableApplierByteBufConverter();
    }

    @Override
    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    protected void initMetaSchema() throws SQLException {
        DataSource dataSource = DataSourceManager.getInstance().getDataSource(endPoint);
        try (Connection connection = dataSource.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(CREATE_MONITOR_DATABASE);
                statement.execute(CREATE_GTID_EXECUTED_TABLE);
            }
        }
    }
}
