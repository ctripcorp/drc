package com.ctrip.framework.drc.monitor.function.cases;

import com.ctrip.framework.drc.monitor.function.operator.WriteSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:57.
 */
public interface WriteCase extends InsertCase, UpdateCase, DeleteCase {

    boolean executeWrite(WriteSqlOperator sqlOperator);
}
