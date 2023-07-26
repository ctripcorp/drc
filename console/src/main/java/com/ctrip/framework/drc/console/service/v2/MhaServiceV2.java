package com.ctrip.framework.drc.console.service.v2;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;

import java.util.List;
import java.util.Map;

public interface MhaServiceV2 {
    Map<String, MhaTblV2> queryMhaByNames(List<String> mhaNames);
    Map<Long, MhaTblV2> queryMhaByIds(List<Long> mhaNames);
}
