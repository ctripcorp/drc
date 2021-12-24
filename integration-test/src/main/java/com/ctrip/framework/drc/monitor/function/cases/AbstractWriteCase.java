package com.ctrip.framework.drc.monitor.function.cases;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.operator.WriteSqlOperator;

/**
 * Created by mingdongli
 * 2019/10/13 上午8:50.
 */
public abstract class AbstractWriteCase extends AbstractCase implements WriteCase {

    public AbstractWriteCase(Execution execution) {
        super(execution);
    }

    @Override
    public boolean executeWrite(WriteSqlOperator sqlOperator) {
        return sqlOperator.write(execution);
    }

    @Override
    public boolean executeDelete(WriteSqlOperator sqlOperator) {
        return sqlOperator.delete(execution);
    }

    @Override
    public boolean executeInsert(WriteSqlOperator sqlOperator) {
        return sqlOperator.insert(execution);
    }

    @Override
    public int[] executeBatchInsert(WriteSqlOperator sqlOperator) {
        return sqlOperator.batchInsert(execution);
    }

    @Override
    public boolean executeUpdate(WriteSqlOperator sqlOperator) {
        return sqlOperator.update(execution);
    }
}
