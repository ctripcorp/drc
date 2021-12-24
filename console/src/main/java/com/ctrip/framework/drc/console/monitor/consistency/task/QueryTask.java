package com.ctrip.framework.drc.console.monitor.consistency.task;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;

/**
 * Created by mingdongli
 * 2019/11/15 下午4:32.
 */
public interface QueryTask<R> {

    R calculate(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst);
}
