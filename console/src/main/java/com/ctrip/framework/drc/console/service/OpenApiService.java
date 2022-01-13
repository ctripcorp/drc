package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.vo.MhaGroupFilterVo;

import java.sql.SQLException;
import java.util.List;

public interface OpenApiService {

    List<MhaGroupFilterVo> getAllDrcMhaDbFilters() throws SQLException;
}
