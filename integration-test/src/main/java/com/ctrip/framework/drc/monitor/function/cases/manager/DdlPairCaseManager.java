package com.ctrip.framework.drc.monitor.function.cases.manager;

import com.ctrip.framework.drc.core.monitor.cases.AbstractPairCase;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;

/**
 * @Author limingdong
 * @create 2020/3/19
 */
public class DdlPairCaseManager extends AbstractPairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> implements PairCaseManager<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    @Override
    public void addPairCase(PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> pairCase) {
        pairCases.add(pairCase);
    }
}
