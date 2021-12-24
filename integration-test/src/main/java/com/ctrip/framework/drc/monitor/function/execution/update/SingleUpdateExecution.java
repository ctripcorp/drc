package com.ctrip.framework.drc.monitor.function.execution.update;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.execution.TransactionExecution;

/**
 * Created by mingdongli
 * 2019/10/13 上午10:31.
 */
public class SingleUpdateExecution extends TransactionExecution implements Execution {

    public SingleUpdateExecution(String statements) {
        this.statements.add(statements);
    }

}
