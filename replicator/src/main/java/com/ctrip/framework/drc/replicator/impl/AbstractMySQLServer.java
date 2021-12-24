package com.ctrip.framework.drc.replicator.impl;

import com.ctrip.framework.drc.replicator.MySQLServer;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by mingdongli
 * 2019/9/21 上午11:07.
 */
public abstract class AbstractMySQLServer extends AbstractLifecycle implements MySQLServer {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

}
