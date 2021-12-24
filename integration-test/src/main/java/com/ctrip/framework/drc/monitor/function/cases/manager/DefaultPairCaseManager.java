package com.ctrip.framework.drc.monitor.function.cases.manager;

import com.ctrip.framework.drc.core.monitor.cases.AbstractPairCase;
import com.ctrip.framework.drc.core.monitor.cases.PairCase;
import com.ctrip.framework.drc.core.monitor.operator.ReadSqlOperator;
import com.ctrip.framework.drc.monitor.function.operator.ReadWriteSqlOperator;
import com.dianping.cat.message.Transaction;

import java.sql.ResultSet;

/**
 * Created by mingdongli
 * 2019/10/10 上午12:11.
 */
public class DefaultPairCaseManager extends AbstractPairCase<ReadWriteSqlOperator, ReadSqlOperator<ResultSet>> implements PairCaseManager<ReadWriteSqlOperator, ReadSqlOperator<ResultSet>> {

    @Override
    public void addPairCase(PairCase<ReadWriteSqlOperator, ReadSqlOperator<ResultSet>> pairCase) {
        pairCases.add(pairCase);
    }
}
