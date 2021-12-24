package com.ctrip.framework.drc.core.driver.command;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author wenchao.meng
 * <p>
 * Sep 01, 2019
 */
public interface MySQLCommand<V> extends Command<V> {

    SERVER_COMMAND getCommandType();
}
