package com.ctrip.framework.drc.monitor.function.cases;

import com.ctrip.framework.drc.monitor.function.operator.WriteSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:56.
 */
public interface InsertCase extends Case {

    boolean executeInsert(WriteSqlOperator sqlOperator);

    int[] executeBatchInsert(WriteSqlOperator sqlOperator);
}
