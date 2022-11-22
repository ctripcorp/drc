package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import static com.ctrip.framework.drc.core.driver.binlog.manager.TableOperationManager.queryTableComment;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;

/**
 * @Author limingdong
 * @create 2022/11/22
 */
public class CommentQueryTask extends AbstractSchemaTask<String> implements NamedCallable<String> {

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
        DDL_LOGGER.info("[Comment] query start");
        return queryTableComment(inMemoryDataSource.getConnection(), schema, table);
    }

}
