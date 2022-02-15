package com.ctrip.framework.drc.manager.healthcheck.service.task;

import com.ctrip.framework.drc.core.driver.healthcheck.task.AbstractQueryTask;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.google.common.collect.Lists;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * only for monitor and alarm, due to no password
 * Created by mingdongli
 * 2019/11/22 上午11:05.
 */
public class SlaveFetcherQueryTask extends AbstractQueryTask<List<String>> {

    private static final String SLAVE_LIST = "select HOST from information_schema.processlist as p where p.command = 'Binlog Dump GTID' and p.user = 'usvr_replication';";

    public SlaveFetcherQueryTask(Endpoint master) {
        super(master);
    }

    @Override
    protected List<String> doQuery() {
        return fetchSlaveHosts(master);
    }

    @SuppressWarnings("findbugs:RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
    protected List<String> fetchSlaveHosts(Endpoint endpoint) {
        DataSource dataSource = dataSourceManager.getDataSource(endpoint);
        List<String> res = Lists.newArrayList();
        try (final Connection connection = dataSource.getConnection()){
            final Statement statement = connection.createStatement();

            final ResultSet readOnlyResultSet = statement.executeQuery(SLAVE_LIST);
            if (readOnlyResultSet.next()) {
                String host = readOnlyResultSet.getString("HOST");
                res.add(host);
            }

            readOnlyResultSet.close();
            statement.close();
        } catch (Exception e) {
            logger.error("fetchSlaveHosts of master {}:{} error", endpoint.getHost(), endpoint.getPort(), e);
        }
        return res;
    }
}