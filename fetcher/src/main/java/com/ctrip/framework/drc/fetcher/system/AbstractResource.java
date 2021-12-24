package com.ctrip.framework.drc.fetcher.system;

import com.ctrip.framework.drc.fetcher.system.qconfig.AbstractUnitUsingQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Slight
 * Sep 18, 2019
 */
public abstract class AbstractResource extends AbstractUnitUsingQConfig implements Resource, Resource.Dynamic {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
}
