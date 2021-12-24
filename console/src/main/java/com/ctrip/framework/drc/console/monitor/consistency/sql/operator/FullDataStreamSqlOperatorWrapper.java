package com.ctrip.framework.drc.console.monitor.consistency.sql.operator;

import com.ctrip.xpipe.api.endpoint.Endpoint;

import static com.ctrip.framework.drc.console.monitor.consistency.utils.Constant.DEFAULT_FETCH_SIZE;
import static com.ctrip.framework.drc.console.monitor.consistency.utils.Constant.QUERY_TIMEOUT_INSECOND;

/**
 * Created by jixinwang on 2021/2/20
 */
public class FullDataStreamSqlOperatorWrapper extends StreamSqlOperatorWrapper {
    public FullDataStreamSqlOperatorWrapper(Endpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        readSqlOperator = new StreamQuerySqlOperator(endpoint, poolProperties, DEFAULT_FETCH_SIZE, QUERY_TIMEOUT_INSECOND);
        readSqlOperator.initialize();
    }
}
