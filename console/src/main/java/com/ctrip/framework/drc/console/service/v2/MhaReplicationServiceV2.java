package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dto.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTask;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.core.http.PageResult;

import java.util.List;

public interface MhaReplicationServiceV2 {
    PageResult<MhaReplicationTbl> queryByPage(MhaReplicationQuery query);

    List<MhaReplicationTbl> queryRelatedReplications(List<Long> relatedMhaId);


    /**
     * 获取 srcMha -> dstMha 该同步链路延迟
     * <p>
     * 该延迟非精准实时延迟，误差1s
     *
     * @see PeriodicalUpdateDbTask#scheduledTask()
     * @see com.ctrip.framework.drc.console.monitor.delay.server.StaticDelayMonitorServer
     */
    MhaDelayInfoDto getMhaReplicationDelay(String srcMha, String dstMha);
}
