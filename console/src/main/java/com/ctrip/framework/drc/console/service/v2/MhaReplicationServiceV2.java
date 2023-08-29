package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dto.v2.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.dto.v2.MhaReplicationDto;
import com.ctrip.framework.drc.console.monitor.delay.task.PeriodicalUpdateDbTask;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.core.http.PageResult;

import java.util.List;

public interface MhaReplicationServiceV2 {
    PageResult<MhaReplicationTbl> queryByPage(MhaReplicationQuery query);

    List<MhaReplicationTbl> queryRelatedReplications(List<Long> relatedMhaId);

    List<MhaReplicationDto> queryRelatedReplicationList(List<String> mhaNames);
    List<MhaReplicationDto> queryRelatedReplications(List<String> mhaNames, List<String> dbNames);
    List<MhaReplicationDto> queryReplicationByIds(List<Long> replicationIds);

    /**
     * 获取 srcMha -> dstMha 该同步链路延迟
     * <p>
     * 用于判断DB是否可搬迁，非精准实时延迟，误差1s。通过直连DB查询。
     *
     * @see PeriodicalUpdateDbTask#scheduledTask()
     * @see com.ctrip.framework.drc.console.monitor.delay.server.StaticDelayMonitorServer
     */
    MhaDelayInfoDto getMhaReplicationDelay(String srcMha, String dstMha);

    /**
     * 批量获取延迟
     */
    List<MhaDelayInfoDto> getMhaReplicationDelays(List<MhaReplicationDto> mhaReplicationDtoList);
}
