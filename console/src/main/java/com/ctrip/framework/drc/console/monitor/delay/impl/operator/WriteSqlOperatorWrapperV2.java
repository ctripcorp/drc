package com.ctrip.framework.drc.console.monitor.delay.impl.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.StatementExecutorResult;
import com.ctrip.framework.drc.core.monitor.operator.WriteBaseSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.WriteSqlOperatorV2;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import org.apache.tomcat.jdbc.pool.DataSource;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/12/28 11:43
 */
public class WriteSqlOperatorWrapperV2 extends WriteBaseSqlOperator implements WriteSqlOperatorV2 {

    private WriteSqlOperatorV2 writeSqlOperatorV2;

    public WriteSqlOperatorWrapperV2(Endpoint endpoint) {
        super(endpoint, null);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

        writeSqlOperatorV2 = new DefaultWriteSqlOperator(endpoint, poolProperties);
        writeSqlOperatorV2.initialize();
    }

    @Override
    protected void doStart() throws Exception {
        writeSqlOperatorV2.start();
    }

    @Override
    protected void doStop() throws Exception {
        writeSqlOperatorV2.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        writeSqlOperatorV2.dispose();
    }


    @Override
    public StatementExecutorResult writeWithResult(Execution execution) throws SQLException {
        return writeSqlOperatorV2.writeWithResult(execution);

    }

    public DataSource getDataSource() {
        return writeDataSource;
    }

}
