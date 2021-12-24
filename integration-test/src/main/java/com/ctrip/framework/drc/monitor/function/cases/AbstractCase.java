package com.ctrip.framework.drc.monitor.function.cases;

import com.ctrip.framework.drc.core.monitor.execution.Execution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/10/9 下午10:46.
 */
public abstract class AbstractCase implements Case {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected Execution execution;

    public AbstractCase(Execution execution) {
        this.execution = execution;
    }
}
