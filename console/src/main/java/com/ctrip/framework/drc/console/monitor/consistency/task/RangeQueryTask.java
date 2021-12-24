package com.ctrip.framework.drc.console.monitor.consistency.task;

import com.ctrip.framework.drc.console.monitor.consistency.cases.Row;
import com.ctrip.framework.drc.console.monitor.consistency.cases.StreamQueryCheckPairCase;
import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.StreamRangeQuerySingleExecution;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;
import com.ctrip.framework.drc.console.monitor.consistency.table.TimeConcern;
import com.ctrip.framework.drc.core.monitor.entity.ConsistencyEntity;
import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * Created by mingdongli
 * 2019/11/15 下午4:40.
 */
public class RangeQueryTask extends AbstractQueryTask implements QueryTask<Map<String, Row>> {

    protected Logger logger = LoggerFactory.getLogger("consistencyMonitorLogger");

    public RangeQueryTask(TableNameDescriptor tableDescriptor) {
        awareExecution = new StreamRangeQuerySingleExecution(tableDescriptor);
        comparablePairCase = new StreamQueryCheckPairCase(awareExecution);
    }

    public RangeQueryTask(TableNameDescriptor tableDescriptor, ConsistencyEntity consistencyEntity) {
        awareExecution = new StreamRangeQuerySingleExecution(tableDescriptor);
        comparablePairCase = new StreamQueryCheckPairCase(awareExecution, consistencyEntity);
    }

    @Override
    public Map<String, Row> calculate(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst) {

        if (awareExecution instanceof TimeConcern) {
            TimeConcern timeConcern = (TimeConcern) awareExecution;
            Date now = new Date();
            timeConcern.setTime(now);
            logger.info("[Time] is updated to {}", now);
        }
        Map<String, Row> res = super.calculate(src, dst);
        logger.info("[Calculate] total diffRow size is {} in {}", res.size(), this.getClass().getSimpleName());
        return res;
    }

}
