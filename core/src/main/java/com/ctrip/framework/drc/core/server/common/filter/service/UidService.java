package com.ctrip.framework.drc.core.server.common.filter.service;

import com.ctrip.framework.drc.core.server.common.filter.row.RowsFilterResult;
import com.ctrip.framework.drc.core.server.common.filter.row.UserContext;
import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * @Author limingdong
 * @create 2022/5/10
 */
public interface UidService extends Ordered {

    RowsFilterResult.Status filterUid(UserContext uidContext) throws Exception;

    RowsFilterResult.Status filterUdl(UserContext uidContext) throws Exception;

}
