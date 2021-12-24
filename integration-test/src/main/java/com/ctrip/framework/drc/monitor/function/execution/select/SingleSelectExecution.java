package com.ctrip.framework.drc.monitor.function.execution.select;

import com.ctrip.framework.drc.monitor.function.execution.AbstractExecution;
import com.ctrip.framework.drc.core.monitor.execution.Execution;

/**
 * Created by mingdongli
 * 2019/10/9 下午11:08.
 */
public class SingleSelectExecution extends AbstractExecution implements Execution {

    public SingleSelectExecution(String statements) {
        this.statements.add(statements);
    }

}
