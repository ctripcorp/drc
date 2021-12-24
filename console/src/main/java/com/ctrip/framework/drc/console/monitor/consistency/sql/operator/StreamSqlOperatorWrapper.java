package com.ctrip.framework.drc.console.monitor.consistency.sql.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.BaseSqlOperator;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.sql.SQLException;

import static com.ctrip.framework.drc.console.monitor.consistency.utils.Constant.DEFAULT_FETCH_SIZE;
import static com.ctrip.framework.drc.console.monitor.consistency.utils.Constant.QUERY_TIMEOUT_INSECOND;

public class StreamSqlOperatorWrapper extends BaseSqlOperator implements ReadSqlOperator<ReadResource> {

    protected ReadSqlOperator<ReadResource> readSqlOperator;

    public StreamSqlOperatorWrapper(Endpoint endpoint) {
        super(endpoint, null);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();

        readSqlOperator = new StreamQuerySqlOperator(endpoint, poolProperties, DEFAULT_FETCH_SIZE, QUERY_TIMEOUT_INSECOND);
        readSqlOperator.initialize();
    }

    @Override
    public ReadResource select(Execution execution) throws SQLException {
        return readSqlOperator.select(execution);
    }

}
