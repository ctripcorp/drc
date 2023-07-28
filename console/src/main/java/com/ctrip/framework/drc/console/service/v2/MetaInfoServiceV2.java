package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;

import java.util.List;

public interface MetaInfoServiceV2 {

    List<BuTbl> queryAllBu();

    List<RegionTbl> queryAllRegion();
}
