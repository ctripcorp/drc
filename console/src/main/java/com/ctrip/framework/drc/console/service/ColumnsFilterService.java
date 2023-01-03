package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.ColumnsFilterConfigDto;
import com.ctrip.framework.drc.console.vo.ColumnsFilterMappingVo;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jixinwang on 2022/12/30
 */
public interface ColumnsFilterService {

    String addColumnsFilterConfig(ColumnsFilterConfigDto rowsFilterConfigDto) throws SQLException;

    String updateColumnsFilterConfig(ColumnsFilterConfigDto rowsFilterConfigDto) throws SQLException;

    String deleteColumnsFilterConfig(Long id) throws SQLException;

    List<ColumnsFilterMappingVo> getColumnsFilterMappingVos(Long applierGroupId, int applierType) throws SQLException;
}
