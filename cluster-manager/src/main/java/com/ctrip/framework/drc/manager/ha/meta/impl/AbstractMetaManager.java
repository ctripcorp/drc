package com.ctrip.framework.drc.manager.ha.meta.impl;

import com.ctrip.framework.drc.core.server.utils.MetaClone;
import com.ctrip.framework.drc.manager.ha.meta.DrcManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public abstract class AbstractMetaManager implements DrcManager {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected <T extends Serializable> T clone(T obj){
        return MetaClone.clone(obj);
    }

}
