package com.ctrip.framework.drc.monitor.function.cases;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:59.
 */
public interface SelectCase extends Case {

    ReadResource executeSelect(ReadSqlOperator<ReadResource> readSqlOperator);
}
