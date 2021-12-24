package com.ctrip.framework.drc.monitor.function.execution.insert;

import com.ctrip.framework.drc.monitor.function.execution.TransactionExecution;
import com.ctrip.framework.drc.monitor.function.execution.WriteExecution;

import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/9 下午11:24.
 */
public class MultiStatementWriteExecution extends TransactionExecution implements WriteExecution {

    public MultiStatementWriteExecution(List<String> statements) {
        this.statements = statements;
    }

}
