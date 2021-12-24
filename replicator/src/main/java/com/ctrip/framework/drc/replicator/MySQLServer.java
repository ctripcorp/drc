package com.ctrip.framework.drc.replicator;

import com.ctrip.framework.drc.core.driver.command.handler.CommandHandler;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface MySQLServer extends Lifecycle {

    void addCommandHandler(CommandHandler handler);
}
