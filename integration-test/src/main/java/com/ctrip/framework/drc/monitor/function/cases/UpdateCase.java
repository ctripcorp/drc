package com.ctrip.framework.drc.monitor.function.cases;

import com.ctrip.framework.drc.monitor.function.operator.WriteSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/13 上午8:47.
 */
public interface UpdateCase extends Case {

    boolean executeUpdate(WriteSqlOperator sqlOperator);
}
