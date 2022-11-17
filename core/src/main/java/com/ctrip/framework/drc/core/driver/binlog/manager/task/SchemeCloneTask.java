package com.ctrip.framework.drc.core.driver.binlog.manager.task;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.manager.TablePartitionManager;
import com.ctrip.framework.drc.core.driver.binlog.manager.exception.DdlException;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.util.List;
import java.util.Map;

import static com.ctrip.framework.drc.core.server.config.SystemConfig.DDL_LOGGER;
import static com.ctrip.framework.drc.core.server.config.SystemConfig.SEMICOLON;

/**
 * @Author limingdong
 * @create 2021/4/7
 */
public class SchemeCloneTask extends AbstractSchemaTask<Boolean> implements NamedCallable<Boolean> {

    private Map<String, Map<String, String>> ddlSchemas;

    private String registerKey;

    public SchemeCloneTask(Map<String, Map<String, String>> ddlSchemas, Endpoint inMemoryEndpoint, DataSource inMemoryDataSource, String registerKey) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.ddlSchemas = ddlSchemas;
        this.registerKey = registerKey;
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
                Pair<Boolean, String> transformRes = TablePartitionManager.transformCreatePartition(tableCreate);
                if (transformRes.getKey()) {
                    DDL_LOGGER.info("[Transform] partition from {} to {} in {}", tableCreate, transformRes.getValue(), getClass().getSimpleName());
                    tableCreate = transformRes.getValue();
                }
                sqls.add(trim(tableCreate));
            }
        }
        int concurrency = DynamicConfig.getInstance().getConcurrency(registerKey);
        DDL_LOGGER.info("[SchemeCloneTask] the concurrency of {} is: {}", registerKey, concurrency);
        res = doCreate(sqls, TableCreateTask.class, false, concurrency);
        if (!res) {
            throw new DdlException(null);
        }
        return res;
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
