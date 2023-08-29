package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.core.entity.Drc;

import java.sql.SQLException;
import java.util.List;

public interface MetaInfoServiceV2 {

    List<BuTbl> queryAllBu();

    List<BuTbl> queryAllBuWithCache();

    List<RegionTbl> queryAllRegion();

    List<RegionTbl> queryAllRegionWithCache();

    List<DcDo> queryAllDc();

    List<DcDo> queryAllDcWithCache();

    Integer findAvailableApplierPort(String ip) throws SQLException;

    Drc getDrcReplicationConfig(Long replicationId);
    Drc getDrcReplicationConfig(String srcMhaName, String dstMhaName);

    Drc getDrcMessengerConfig(String mhaName);
}
