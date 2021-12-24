package com.ctrip.framework.drc.monitor.function.execution;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/10 上午9:08.
 */
public abstract class AbstractExecution implements Execution {

    protected List<String> statements = Lists.newArrayList();

    @Override
    public Collection<String> getStatements() {
        return statements;
    }

    @Override
    public boolean appendStatements(Collection<String> statement) {
        return statements.addAll(statement);
    }

    @Override
    public boolean replaceStatements(Collection<String> statement) {
        statements.clear();
        return appendStatements(statement);
    }
}
