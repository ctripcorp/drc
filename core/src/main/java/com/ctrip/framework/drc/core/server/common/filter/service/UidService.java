package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.framework.drc.core.server.common.filter.row.UidContext;
import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public interface UidService extends Ordered {

    boolean filterUid(UidContext uidContext) throws Exception;

}
