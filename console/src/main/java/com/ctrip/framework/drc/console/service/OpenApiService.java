package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.vo.api.MessengerInfo;
import com.ctrip.framework.drc.console.vo.api.MhaGroupFilterVo;

import java.sql.SQLException;
import java.util.List;

public interface OpenApiService {

    List<MhaGroupFilterVo> getAllDrcMhaDbFilters() throws SQLException;

    List<MessengerInfo> getAllMessengersInfo() throws SQLException;
}
