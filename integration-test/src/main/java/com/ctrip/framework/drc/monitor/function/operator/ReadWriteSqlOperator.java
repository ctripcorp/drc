package com.ctrip.framework.drc.monitor.function.operator;

import com.ctrip.framework.drc.core.monitor.operator.ReadResource;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/9 5:40.
 */
public interface ReadWriteSqlOperator extends ReadSqlOperator<ReadResource>, WriteSqlOperator {

}
