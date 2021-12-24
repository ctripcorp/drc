package com.ctrip.framework.drc.console.service;

import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.core.service.dal.DalClusterTypeEnum;
import com.ctrip.framework.drc.console.pojo.Mha;
import com.ctrip.framework.drc.console.service.impl.DalServiceImpl;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.foundation.Env;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author shenhaibo
 * @version 1.0
 * date: 2020-04-13
 */
public interface DalService {

    List<Mha> getMhasFromDal(String clusterName);

    JsonNode getDalClusterInfo(String dalClusterName, String env);

    Map<String, String> getInstanceGroupsInfo(List<String> mhas, Env env);

    List<DalServiceImpl.DalClusterInfoWrapper> getDalClusterInfoWrappers(Set<String> dalClusterNames, String env);

    ApiResult switchDalClusterType(String dalClusterName, String env, DalClusterTypeEnum typeEnum, String zoneId) throws Exception;

    Map<String, MhaInstanceGroupDto> getMhaList(Env env);

    String getDc(String mha, Env env);
}
