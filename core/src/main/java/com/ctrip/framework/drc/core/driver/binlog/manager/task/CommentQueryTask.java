package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2022/11/22
 */
public class CommentQueryTask extends AbstractSchemaTask<String> implements NamedCallable<String> {

    public static final String SELECT_TABLE_COMMENT =
            "SELECT table_comment FROM INFORMATION_SCHEMA.TABLES WHERE table_schema='%s' AND table_name='%s'";

    private String schema;

    private String table;

    public CommentQueryTask(String schema, String table, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.schema = schema;
        this.table = table;
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public String call() throws Exception {
        DDL_LOGGER.info("[Query] comment start for {}:{}", schema, table);
        try (Connection connection = inMemoryDataSource.getConnection()) {
            try(Statement statement = connection.createStatement()) {
                try(ResultSet resultSet = statement.executeQuery(String.format(SELECT_TABLE_COMMENT, schema, table))) {
                    if (resultSet.next()) {
                        return resultSet.getString(1);
                    } else {
                        return StringUtils.EMPTY;
                    }
                }
            }
        }
    }

}
