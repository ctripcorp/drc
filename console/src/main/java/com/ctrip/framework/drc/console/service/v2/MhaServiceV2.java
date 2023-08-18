package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.vo.check.DrcBuildPreCheckVo;

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
    Map<Long, MhaTblV2> queryMhaByIds(List<Long> mhaIds);
    List<String> getMhaReplicators(String mhaName) throws Exception;
    List<String> getMhaMessengers(String mhaName);

    List<String> getMhaAvailableResource(String mhaName, int type) throws Exception;
    String getMysqlUuid(String mhaName, String ip, int port, boolean master) throws Exception;
    boolean recordMhaInstances(MhaInstanceGroupDto dto) throws Exception;

    DrcBuildPreCheckVo preCheckBeReplicatorIps(String mhaName, List<String> replicatorIps) throws Exception;
}
