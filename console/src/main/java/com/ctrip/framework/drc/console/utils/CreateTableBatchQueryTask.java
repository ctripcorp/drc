package com.ctrip.framework.drc.console.utils;

import com.ctrip.framework.drc.core.config.DynamicConfig;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.AbstractSchemaTask;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.NamedCallable;
import com.ctrip.framework.drc.core.driver.binlog.manager.task.SchemaSnapshotTaskV2;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.tuple.Triple;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * SchemaSnapshotTask concurrent version
 */
public class CreateTableBatchQueryTask extends AbstractSchemaTask<Map<String, Map<String, String>>> implements NamedCallable<Map<String, Map<String, String>>> {

    private static final Logger log = LoggerFactory.getLogger(CreateTableBatchQueryTask.class);
    private final ExecutorService service = ThreadUtils.newCachedThreadPool("CreateTableBatchQueryTask");


    private final List<MySqlUtils.TableSchemaName> tables;

    public CreateTableBatchQueryTask(Endpoint inMemoryEndpoint, DataSource inMemoryDataSource, List<MySqlUtils.TableSchemaName> tables) {
        super(inMemoryEndpoint, inMemoryDataSource);
        this.tables = tables;
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    @Override
    public Map<String, Map<String, String>> call() throws SQLException {
        Map<String, Map<String, String>> res = Maps.newConcurrentMap();
        List<Callable<Triple<String, String, String>>> allTasks = Lists.newArrayList();


        for (MySqlUtils.TableSchemaName table : tables) {
            allTasks.add(new SchemaSnapshotTaskV2.CreateTableQueryTask(inMemoryDataSource, table.getSchema(), table.getName()));
        }
        int concurrency = 50;
        try {
            List<List<Callable<Triple<String, String, String>>>> taskPartitions = Lists.partition(allTasks, concurrency);
            for (List<Callable<Triple<String, String, String>>> subTasks : taskPartitions) {
                List<Future<Triple<String, String, String>>> futures = service.invokeAll(subTasks);
                for (Future<Triple<String, String, String>> future : futures) {
                    Triple<String, String, String> result = future.get(60, TimeUnit.SECONDS);
                    if (result == null || StringUtils.isEmpty(result.getLast())) {
                        continue;
                    }
                    String schema = result.getFirst();
                    String table = result.getMiddle();
                    String createTable = result.getLast();
                    res.computeIfAbsent(schema, e -> new HashMap<>()).put(table, createTable);
                }
            }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            log.error("CreateTableBatchQueryTask", e);
            throw new RuntimeException(e);
        }
        return res;
    }

    @Override
    public void afterException(Throwable t) {
        super.afterException(t);
    }

}
