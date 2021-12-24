package com.ctrip.framework.drc.monitor.function.cases.manager;

import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/10 上午12:14.
 */
public interface PairCaseManager<S extends ReadSqlOperator, D extends ReadSqlOperator> extends PairCase<S, D> {

    void addPairCase(PairCase<S, D> pairCase);
}
