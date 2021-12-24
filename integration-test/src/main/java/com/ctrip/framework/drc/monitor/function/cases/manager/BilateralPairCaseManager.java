package com.ctrip.framework.drc.monitor.function.cases.manager;

import com.ctrip.framework.drc.core.monitor.cases.AbstractPairCase;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;

import java.sql.ResultSet;

public class BilateralPairCaseManager extends AbstractPairCase<ReadWriteSqlOperator, ReadSqlOperator<ResultSet>> implements PairCaseManager<ReadWriteSqlOperator, ReadSqlOperator<ResultSet>> {

    @Override
    public void addPairCase(PairCase<ReadWriteSqlOperator, ReadSqlOperator<ResultSet>> pairCase) {
        pairCases.add(pairCase);
    }
}