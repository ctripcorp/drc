package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.xpipe.api.lifecycle.Ordered;

import java.util.Set;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public interface UidService extends Ordered {

    boolean filterUid(String uid, Set<String> locations, boolean illegalArgument) throws Exception;

}
