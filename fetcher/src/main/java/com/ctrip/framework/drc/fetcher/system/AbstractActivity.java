package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.system.qconfig.AbstractUnitUsingQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public abstract class AbstractActivity extends AbstractUnitUsingQConfig implements Activity {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public abstract void doStart() throws Exception;

    public abstract void doStop() throws Exception;
}
