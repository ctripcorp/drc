package com.ctrip.framework.drc.monitor.function.operator;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.core.monitor.operator.SqlOperator;

/**
 * Created by mingdongli
 * 2019/10/9 下午5:39.
 */
public interface WriteSqlOperator extends SqlOperator {

    /**
     * write 包含insert、update、delete
     * @param execution
     * @return
     */
    boolean write(Execution execution);

    boolean insert(Execution execution);

    int[] batchInsert(Execution execution);

    boolean update(Execution execution);

    boolean delete(Execution execution);
}
