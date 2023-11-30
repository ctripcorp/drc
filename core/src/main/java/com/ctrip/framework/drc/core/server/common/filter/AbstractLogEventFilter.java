package com.ctrip.framework.drc.core.server.common.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/10/9 上午10:55.
 */
public abstract class AbstractLogEventFilter<T> extends AbstractFilter<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

}
