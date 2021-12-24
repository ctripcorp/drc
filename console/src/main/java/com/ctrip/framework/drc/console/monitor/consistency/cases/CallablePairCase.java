package com.ctrip.framework.drc.console.monitor.consistency.cases;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;

/**
 * Created by mingdongli
 * 2019/11/15 下午2:40.
 */
public interface CallablePairCase<R> {

    R test(ReadSqlOperator<ReadResource> src, ReadSqlOperator<ReadResource> dst);
}
