package com.ctrip.framework.drc.console.monitor.delay.impl.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.BaseSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadWriteSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.SQLException;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2019-12-05
 */
public class WriteSqlOperatorWrapper extends BaseSqlOperator implements ReadWriteSqlOperator {

    private ReadWriteSqlOperator readWriteSqlOperator;

    public WriteSqlOperatorWrapper(Endpoint endpoint) {
        super(endpoint, null);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

        readWriteSqlOperator = new DelayMonitorSqlOperator(endpoint, poolProperties);
        readWriteSqlOperator.initialize();
    }

    @Override
    protected void doStart() throws Exception {
        readWriteSqlOperator.start();
    }

    @Override
    protected void doStop() throws Exception {
        readWriteSqlOperator.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        readWriteSqlOperator.dispose();
    }

    @Override
    public ReadResource select(Execution execution) throws SQLException {
        return readWriteSqlOperator.select(execution);
    }

    @Override
    public void write(Execution execution) throws SQLException {
        readWriteSqlOperator.write(execution);
    }

    @Override
    public StatementExecutorResult writeWithResult(Execution execution) throws SQLException {
        return readWriteSqlOperator.writeWithResult(execution);
    }

    @Override
    public void insert(Execution execution) throws SQLException {
        readWriteSqlOperator.insert(execution);
    }

    @Override
    public void update(Execution execution) throws SQLException {
        readWriteSqlOperator.update(execution);
    }

    @Override
    public void delete(Execution execution) throws SQLException {
        readWriteSqlOperator.delete(execution);
    }
    
    public DataSource getDataSource() {
        return dataSource;
    }
}
