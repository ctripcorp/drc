package com.ctrip.framework.drc.console.service.v2.resource;

import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.core.http.ApiResult;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/12/6 20:26
 */
public interface RouteService {

    String submitProxyRouteConfig(RouteDto routeDto);

    List<RouteDto> getRoutes(String routeOrgName, String srcDcName, String dstDcName, String tag, Integer deleted);

    ApiResult deleteRoute(String routeOrgName, String srcDcName, String dstDcName, String tag);
}
