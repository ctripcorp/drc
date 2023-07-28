package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.core.http.PageResult;

import java.util.List;

public interface MhaReplicationServiceV2 {
    PageResult<MhaReplicationTbl> queryByPage(MhaReplicationQuery query);

    List<MhaReplicationTbl> queryRelatedReplications(Long relatedMhaId);
}
