package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.SEMICOLON;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeCloneTask extends AbstractSchemaTask implements NamedCallable<Boolean> {

    private Map<String, Map<String, String>> ddlSchemas;

    public SchemeCloneTask(Map<String, Map<String, String>> ddlSchemas, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.ddlSchemas = ddlSchemas;
    }

    @Override
    public void afterException(Throwable t) {
        super.afterException(t);
        new RetryTask<>(new SchemeClearTask(inMemoryEndpoint, inMemoryDataSource)).call();
    }

    @Override
    public Boolean call() throws Exception {
        // create database
        boolean res = doCreate(ddlSchemas.keySet(), DatabaseCreateTask.class, true);
        if (!res) {
            return res;
        }

        // create table
        List<String> sqls = Lists.newArrayList();
        for (Map<String, String> tables : ddlSchemas.values()) {
            for (String tableCreate : tables.values()) {
                sqls.add(trim(tableCreate));
            }
        }
        return doCreate(sqls, TableCreateTask.class, false);
    }

    private String trim(String createTable) {
        String res = createTable.trim();
        do {
            if (res.endsWith(SEMICOLON)) {
                res = res.substring(0, res.length() - 1).trim();
            }
        } while (res.endsWith(SEMICOLON));
        return res;
    }

}
