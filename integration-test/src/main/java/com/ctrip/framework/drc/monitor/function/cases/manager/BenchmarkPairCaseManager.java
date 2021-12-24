package com.ctrip.framework.drc.monitor.function.cases.manager;

import com.ctrip.framework.drc.core.monitor.cases.AbstractPairCase;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;

/**
 * Created by mingdongli
 * 2019/11/1 下午5:24.
 */
public class BenchmarkPairCaseManager extends AbstractPairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> implements PairCaseManager<ReadWriteSqlOperator, ReadWriteSqlOperator> {

    @Override
    public void addPairCase(PairCase<ReadWriteSqlOperator, ReadWriteSqlOperator> pairCase) {
        pairCases.add(pairCase);
    }

    @Override
    public void test(ReadWriteSqlOperator src, ReadWriteSqlOperator dst) {

    }
}
