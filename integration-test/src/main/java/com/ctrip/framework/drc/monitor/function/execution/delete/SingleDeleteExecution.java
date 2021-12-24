package com.ctrip.framework.drc.monitor.function.execution.delete;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.execution.TransactionExecution;

/**
 * Created by mingdongli
 * 2019/10/13 上午10:33.
 */
public class SingleDeleteExecution extends TransactionExecution implements Execution {

    public SingleDeleteExecution(String statements) {
        this.statements.add(statements);
    }

}
