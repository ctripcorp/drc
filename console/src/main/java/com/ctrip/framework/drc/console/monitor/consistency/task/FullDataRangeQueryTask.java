package com.ctrip.framework.drc.console.monitor.consistency.task;

import com.ctrip.framework.drc.console.monitor.consistency.cases.FullDataComparablePairCase;
import com.ctrip.framework.drc.console.monitor.consistency.sql.execution.FullDataTimeRangeQuerySingleExecution;
import com.ctrip.framework.drc.console.monitor.consistency.table.TableNameDescriptor;

/**
 * Created by jixinwang on 2021/2/20
 */
public class FullDataRangeQueryTask extends RangeQueryTask {
    public FullDataRangeQueryTask(TableNameDescriptor tableDescriptor, String startTime, String endTime) {
        super(tableDescriptor);
        awareExecution = new FullDataTimeRangeQuerySingleExecution(tableDescriptor, startTime, endTime);
        comparablePairCase = new FullDataComparablePairCase(awareExecution);
    }
}
