package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.MhaDto;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.xpipe.api.endpoint.Endpoint;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface MhaService {

    List<Map<String, String>> getCachedAllClusterNames();

    List<Map<String, String>> getCachedAllClusterNames(String keyWord);

    List<Map<String,String>> getAllClusterNames();

    Endpoint getMasterMachineInstance(String mha);

    List<Map<String, Object>> getAllDbsAndDals(String clusterName, String env, String zoneId);

    List<Map<String, Object>> getAllDbsAndDals(String clusterName, String env);

    List<String> getAllDbs(String clusterName, String env);

    String getDcForMha(String mha);

    ApiResult recordMha(MhaDto mhaDto);
    
    MhaDto queryMhaInfo(Long mhaId) throws SQLException;
}
