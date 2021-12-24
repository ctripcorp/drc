package com.ctrip.framework.drc.monitor.function.execution;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * Created by mingdongli
 * 2019/10/10 上午9:11.
 */
public abstract class TransactionExecution extends AbstractExecution implements WriteExecution {

    @Override
    public Collection<String> getStatements() {
        List<String> res = Lists.newArrayList();
        res.add("begin");
        res.addAll(super.getStatements());
        res.add("commit");
        return res;
    }

    @Override
    public int affect() {
        return this.statements.size();
    }

}
