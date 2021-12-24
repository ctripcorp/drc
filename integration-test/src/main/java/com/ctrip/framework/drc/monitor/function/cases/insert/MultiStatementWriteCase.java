package com.ctrip.framework.drc.monitor.function.cases.insert;

import com.ctrip.framework.drc.monitor.function.cases.AbstractWriteCase;
import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.operator.WriteSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/9 下午11:28.
 */
public class MultiStatementWriteCase extends AbstractWriteCase {

    public MultiStatementWriteCase(Execution execution) {
        super(execution);
    }

    @Override
    public boolean executeInsert(WriteSqlOperator sqlOperator) {
        return sqlOperator.insert(execution);
    }
}
