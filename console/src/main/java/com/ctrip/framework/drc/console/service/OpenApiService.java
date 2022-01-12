package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.vo.MhaDbFiltersVo;

import java.util.List;
import java.util.Map;

public interface OpenApiService {
   
    List<MhaDbFiltersVo> getAllDrcMhaDbFilters();
}
