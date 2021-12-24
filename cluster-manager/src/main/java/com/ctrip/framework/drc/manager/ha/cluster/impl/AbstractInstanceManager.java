package com.ctrip.framework.drc.manager.ha.cluster.impl;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * response for registering zk and stopping instance
 * @Author limingdong
 * @create 2020/5/5
 */
public abstract class AbstractInstanceManager extends AbstractCurrentMetaObserver {

    @Autowired
    protected InstanceStateController instanceStateController;

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }

}
