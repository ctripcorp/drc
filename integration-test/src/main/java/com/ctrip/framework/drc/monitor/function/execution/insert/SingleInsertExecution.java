package com.ctrip.framework.drc.monitor.function.execution.insert;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.ctrip.framework.drc.monitor.function.execution.TransactionExecution;

/**
 * Created by mingdongli
 * 2019/10/13 上午11:47.
 */
public class SingleInsertExecution extends TransactionExecution implements Execution {

    public SingleInsertExecution(String statements) {
        this.statements.add(statements);
    }

}
