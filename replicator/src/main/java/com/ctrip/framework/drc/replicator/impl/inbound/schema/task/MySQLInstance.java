package com.ctrip.framework.drc.replicator.impl.inbound.schema.task;

import com.ctrip.xpipe.api.lifecycle.Destroyable;

/**
 * @Author limingdong
 * @create 2022/10/26
 */
public interface MySQLInstance extends Destroyable {

    void destroy();
}
