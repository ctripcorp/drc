package com.ctrip.framework.drc.console.service.v2.resource;

import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.dto.RouteMappingDto;
import com.ctrip.framework.drc.console.param.MhaDbReplicationRouteDto;
import com.ctrip.framework.drc.console.param.MhaRouteMappingDto;
import com.ctrip.framework.drc.console.param.RouteQueryParam;
import com.ctrip.framework.drc.console.vo.v2.ApplierReplicationView;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/12/6 20:26
 */
public interface RouteService {

    void submitRoute(RouteDto routeDto) throws SQLException;

    List<RouteDto> getRoutes(RouteQueryParam param) throws SQLException;

    RouteDto getRoute(long routeId) throws Exception;

    void deleteRoute(long routeId) throws Exception;

    void activeRoute(long routeId) throws SQLException;

    void deactivateRoute(long routeId) throws SQLException;

    List<RouteDto> getRoutesByRegion(String srcDcName, String dstDcName) throws SQLException;

    List<RouteDto> getRoutesByDstRegion(String dstDcName) throws SQLException;

    void submitMhaDbReplicationRoutes(MhaDbReplicationRouteDto routeDto) throws SQLException;

    void submitMhaRoutes(MhaRouteMappingDto routeMappingDto) throws SQLException;

    void deleteMhaDbReplicationRoutes(MhaDbReplicationRouteDto routeDto) throws SQLException;

    void deleteMhaRoutes(MhaDbReplicationRouteDto routeDto) throws SQLException;

    List<RouteMappingDto> getRouteMappings(String srcMhaName, String dstMhaName) throws SQLException;

    List<RouteMappingDto> getRouteMappings(String mhaName) throws SQLException;

    List<ApplierReplicationView> getRelatedDbs(long routeId) throws SQLException;

    List<ApplierReplicationView> getRelatedMhas(long routeId) throws SQLException;
}
