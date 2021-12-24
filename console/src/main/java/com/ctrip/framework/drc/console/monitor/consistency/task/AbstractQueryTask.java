package com.ctrip.framework.drc.console.monitor.consistency.task;

import com.ctrip.framework.drc.console.monitor.consistency.cases.ComparablePairCase;
import com.ctrip.framework.drc.console.monitor.consistency.cases.Row;
import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.NameAwareExecution;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by mingdongli
 * 2019/11/15 下午4:33.
 */
public abstract class AbstractQueryTask implements QueryTask<Map<String, Row>> {

    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    protected ComparablePairCase comparablePairCase;

    protected NameAwareExecution awareExecution;

    @Override
    public Map<String, Row> calculate(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst) {
        return comparablePairCase.test(src, dst);
    }

}
