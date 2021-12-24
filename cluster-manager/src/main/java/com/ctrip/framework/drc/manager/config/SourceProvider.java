package com.ctrip.framework.drc.manager.config;

import com.ctrip.framework.drc.core.entity.Dc;

/**
 * @Author limingdong
 * @create 2020/4/21
 */
public interface SourceProvider {

    Dc getDc(String dcId);

}
