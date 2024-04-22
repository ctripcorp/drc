package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.v3.ReplicatorInfoDto;
import com.ctrip.framework.drc.console.param.v2.MhaQueryParam;
import com.ctrip.framework.drc.console.vo.check.DrcBuildPreCheckVo;
import com.ctrip.framework.drc.console.vo.request.MhaQueryDto;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.ctrip.xpipe.tuple.Pair;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface MhaServiceV2 {
    /**
     * conditional query by name, buId, regionId
     * return empty if all query conditions are null
     *
     * @return id -> MhaTblV2 map
     */
    Map<Long, MhaTblV2> query(String containMhaName, Long buId, Long regionId);
    Map<Long, MhaTblV2> query(MhaQueryDto mha);
    Map<Long, MhaTblV2> queryMhaByIds(List<Long> mhaIds);
    List<MhaTblV2> queryRelatedMhaByDbName(List<String> dbNames) throws SQLException;
    List<String> getMhaReplicators(String mhaName) throws Exception;
    List<ReplicatorInfoDto> getMhaReplicatorsV2(String mhaName);
    List<String> getMhaMessengers(String mhaName);

    List<String> getMhaAvailableResource(String mhaName, int type) throws Exception;
    String getMysqlUuid(String mhaName, String ip, int port, boolean master) throws Exception;
    boolean recordMhaInstances(MhaInstanceGroupDto dto) throws Exception;

    DrcBuildPreCheckVo preCheckBeReplicatorIps(String mhaName, List<String> replicatorIps) throws Exception;

    void updateMhaTag(String mhaName, String tag) throws Exception;

    String getMhaDc(String mhaName) throws Exception;
    
    String getRegion(String mhaName) throws SQLException;
    
    // key:mhaName , value: replicator slave delay
    Map<String,Long> getMhaReplicatorSlaveDelay(List<String> mhas) throws Exception;

    List<String> queryMhasWithOutDrc();

    Pair<Boolean,Integer> offlineMhasWithOutDrc(List<String> mhas) throws SQLException;

    // should check no use first
    boolean offlineMha(String mhaName) throws SQLException;

    List<MhaTblV2> queryMhas(MhaQueryParam param) throws Exception;
}
