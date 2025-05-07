package com.ctrip.framework.drc.console.service.v2.resource.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.dto.RouteMappingDto;
import com.ctrip.framework.drc.console.dto.v3.MhaDbReplicationDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.MhaDbReplicationRouteDto;
import com.ctrip.framework.drc.console.param.MhaRouteMappingDto;
import com.ctrip.framework.drc.console.param.RouteQueryParam;
import com.ctrip.framework.drc.console.service.v2.MhaDbReplicationService;
import com.ctrip.framework.drc.console.service.v2.resource.RouteService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.vo.v2.ApplierReplicationView;
import com.ctrip.framework.drc.core.entity.Route;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/12/6 20:27
 */
@Service
public class RouteServiceImpl implements RouteService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private ProxyTblDao proxyTblDao;
    @Autowired
    private RouteTblDao routeTblDao;
    @Autowired
    private DbReplicationRouteMappingTblDao routeMappingTblDao;
    @Autowired
    private DefaultConsoleConfig consoleConfig;
    @Autowired
    private MhaDbReplicationService mhaDbReplicationService;

    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;

    @Override
    public void submitRoute(RouteDto routeDto) throws SQLException {
        Long routeOrgId = StringUtils.isBlank(routeDto.getRouteOrgName()) ? 0L : buTblDao.queryByBuName(routeDto.getRouteOrgName()).getId();
        Long srcDcId = dcTblDao.queryByDcName(routeDto.getSrcDcName()).getId();
        Long dstDcId = dcTblDao.queryByDcName(routeDto.getDstDcName()).getId();
        List<Long> srcProxyIdList = Lists.newArrayList();
        List<Long> relayProxyIdList = Lists.newArrayList();
        List<Long> dstProxyIdList = Lists.newArrayList();

        Map<String, Long> proxyTblMap = proxyTblDao.queryAllExist().stream().collect(Collectors.toMap(ProxyTbl::getUri, ProxyTbl::getId));

        for (String proxyUri : routeDto.getSrcProxyUris()) {
            srcProxyIdList.add(proxyTblMap.get(proxyUri));
        }
        for (String proxyUri : routeDto.getRelayProxyUris()) {
            relayProxyIdList.add(proxyTblMap.get(proxyUri));
        }
        for (String proxyUri : routeDto.getDstProxyUris()) {
            dstProxyIdList.add(proxyTblMap.get(proxyUri));
        }

        String srcProxyIds = StringUtils.join(srcProxyIdList, ",");
        String relayProxyIds = StringUtils.join(relayProxyIdList, ",");
        String dstProxyIds = StringUtils.join(dstProxyIdList, ",");

        if (routeDto.getId() != null && routeDto.getId() > 0L) {  //update
            RouteTbl existRouteTbl = routeTblDao.queryById(routeDto.getId());
            if (existRouteTbl == null) {
                throw ConsoleExceptionUtils.message("route not exist");
            }
            existRouteTbl.setSrcProxyIds(srcProxyIds);
            existRouteTbl.setDstProxyIds(dstProxyIds);
            existRouteTbl.setOptionalProxyIds(relayProxyIds);
            routeTblDao.update(existRouteTbl);
        } else { //insert
            RouteTbl routePojo = RouteTbl.createRoutePojo(routeOrgId, srcDcId, dstDcId, srcProxyIds, relayProxyIds, dstProxyIds, routeDto.getTag());
            RouteTbl existRouteTbl = routeTblDao.queryAllExist().stream().filter(e -> e.equalRoute(routePojo)).findFirst().orElse(null);
            if (existRouteTbl != null) {
                throw ConsoleExceptionUtils.message("route already exist");
            }
            routeTblDao.insert(routePojo);
        }
    }

    @Override
    public List<RouteDto> getRoutes(RouteQueryParam param) throws SQLException {
        if (StringUtils.isNotBlank(param.getSrcDcName())) {
            param.setSrcDcIds(Lists.newArrayList(dcTblDao.queryByDcName(param.getSrcDcName()).getId()));
        }
        if (StringUtils.isNotBlank(param.getDstDcName())) {
            param.setDstDcIds(Lists.newArrayList(dcTblDao.queryByDcName(param.getDstDcName()).getId()));
        }
        List<RouteTbl> routeTbls = routeTblDao.queryByParam(param);
        List<RouteDto> routeDtos = new ArrayList<>();

        List<DbReplicationRouteMappingTbl> routeMappingTbls = routeMappingTblDao.queryAllExist();
        Map<Long, Long> routeMappingCountMap = routeMappingTbls.stream().collect(Collectors.groupingBy(DbReplicationRouteMappingTbl::getRouteId, Collectors.counting()));

        Triple<Map<Long, String>, Map<Long, String>, Map<Long, String>> baseMap = getBaseMap();
        for (RouteTbl routeTbl : routeTbls) {
            RouteDto routeDto = getRouteDto(routeTbl, baseMap.getLeft(), baseMap.getMiddle(), baseMap.getRight());
            routeDto.setRelatedNum(routeMappingCountMap.get(routeTbl.getId()));
            routeDtos.add(routeDto);
        }
        return routeDtos;
    }

    @Override
    public RouteDto getRoute(long routeId) throws Exception {
        RouteTbl routeTbl = routeTblDao.queryById(routeId);
        if (routeTbl == null) {
            return null;
        }
        Triple<Map<Long, String>, Map<Long, String>, Map<Long, String>> baseMap = getBaseMap();
        return getRouteDto(routeTbl, baseMap.getLeft(), baseMap.getMiddle(), baseMap.getRight());
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void deleteRoute(long routeId) throws Exception {
        RouteTbl routeTbl = routeTblDao.queryById(routeId);
        if (null == routeTbl) {
            throw ConsoleExceptionUtils.message("route not exist, routeId: " + routeId);
        }
        routeTbl.setDeleted(BooleanEnum.TRUE.getCode());
        routeTblDao.update(routeTbl);

        deleteRouteMappings(routeId);
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void activeRoute(long routeId) throws SQLException {
        RouteTbl routeTbl = routeTblDao.queryById(routeId);
        if (null == routeTbl) {
            throw ConsoleExceptionUtils.message("route not exist, routeId: " + routeId);
        }
        if (routeTbl.getGlobalActive().equals(BooleanEnum.TRUE.getCode())) {
            return;
        }
        routeTbl.setGlobalActive(BooleanEnum.TRUE.getCode());
        routeTblDao.update(routeTbl);
        deleteRouteMappings(routeId);
    }

    @Override
    public void deactivateRoute(long routeId) throws SQLException {
        RouteTbl routeTbl = routeTblDao.queryById(routeId);
        if (null == routeTbl) {
            throw ConsoleExceptionUtils.message("route not exist, routeId: " + routeId);
        }
        if (routeTbl.getGlobalActive().equals(BooleanEnum.FALSE.getCode())) {
            return;
        }
        routeTbl.setGlobalActive(BooleanEnum.FALSE.getCode());
        routeTblDao.update(routeTbl);
    }

    private void deleteRouteMappings(long routeId) throws SQLException {
        List<DbReplicationRouteMappingTbl> dbReplicationRouteMappingTbls = routeMappingTblDao.queryByRouteId(routeId);
        if (!CollectionUtils.isEmpty(dbReplicationRouteMappingTbls)) {
            dbReplicationRouteMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
            routeMappingTblDao.update(dbReplicationRouteMappingTbls);
        }
    }

    @Override
    public List<RouteDto> getRoutesByRegion(String srcDcName, String dstDcName) throws SQLException {
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        String srcRegionName = dcTbls.stream().filter(e -> e.getDcName().equals(srcDcName)).findFirst().get().getRegionName();
        String dstRegionName = dcTbls.stream().filter(e -> e.getDcName().equals(dstDcName)).findFirst().get().getRegionName();
        List<Long> srcDcIds = dcTbls.stream().filter(e -> e.getRegionName().equals(srcRegionName)).map(DcTbl::getId).toList();
        List<Long> dstDcIds = dcTbls.stream().filter(e -> e.getRegionName().equals(dstRegionName)).map(DcTbl::getId).toList();
        RouteQueryParam queryParam = new RouteQueryParam();
        queryParam.setSrcDcIds(srcDcIds);
        queryParam.setDstDcIds(dstDcIds);
        queryParam.setTag(Route.TAG_META);
        queryParam.setGlobalActive(BooleanEnum.FALSE.getCode());

        List<RouteTbl> routeTbls = routeTblDao.queryByParam(queryParam);
        Triple<Map<Long, String>, Map<Long, String>, Map<Long, String>> baseMap = getBaseMap();
        List<RouteDto> routeDtos = new ArrayList<>();
        for (RouteTbl routeTbl : routeTbls) {
            RouteDto routeDto = getRouteDto(routeTbl, baseMap.getLeft(), baseMap.getMiddle(), baseMap.getRight());
            routeDtos.add(routeDto);
        }
        return routeDtos;
    }

    @Override
    public List<RouteDto> getRoutesByDstRegion(String dstDcName) throws SQLException {
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        String dstRegionName = dcTbls.stream().filter(e -> e.getDcName().equals(dstDcName)).findFirst().get().getRegionName();
        List<Long> dstDcIds = dcTbls.stream().filter(e -> e.getRegionName().equals(dstRegionName)).map(DcTbl::getId).toList();
        RouteQueryParam queryParam = new RouteQueryParam();
        queryParam.setDstDcIds(dstDcIds);
        queryParam.setTag(Route.TAG_CONSOLE);
        queryParam.setGlobalActive(BooleanEnum.FALSE.getCode());
        List<RouteTbl> routeTbls = routeTblDao.queryByParam(queryParam);
        Triple<Map<Long, String>, Map<Long, String>, Map<Long, String>> baseMap = getBaseMap();
        List<RouteDto> routeDtos = new ArrayList<>();
        for (RouteTbl routeTbl : routeTbls) {
            RouteDto routeDto = getRouteDto(routeTbl, baseMap.getLeft(), baseMap.getMiddle(), baseMap.getRight());
            routeDtos.add(routeDto);
        }
        return routeDtos;
    }

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public void submitMhaDbReplicationRoutes(MhaDbReplicationRouteDto routeDto) throws SQLException {
        RouteTbl routeTbl = routeTblDao.queryById(routeDto.getRouteId());
        if (routeTbl.getGlobalActive().equals(BooleanEnum.TRUE.getCode())) {
            throw ConsoleExceptionUtils.message("route is globalActive!");
        }

        List<DbReplicationRouteMappingTbl> existRouteMappings = routeMappingTblDao.queryByMhaDbReplicationIds(routeDto.getMhaDbReplicationIds());
        List<Long> existMhaDbReplicationIds = existRouteMappings.stream().map(DbReplicationRouteMappingTbl::getMhaDbReplicationId).collect(Collectors.toList());

        if (!CollectionUtils.isEmpty(existRouteMappings)) {
            List<DbReplicationRouteMappingTbl> toUpdateRouteMappings = existRouteMappings.stream().filter(e -> !e.getRouteId().equals(routeDto.getRouteId())).collect(Collectors.toList());

            if (!CollectionUtils.isEmpty(toUpdateRouteMappings)) {
                toUpdateRouteMappings.forEach(e -> e.setRouteId(routeDto.getRouteId()));
                routeMappingTblDao.update(toUpdateRouteMappings);
            }

            routeDto.getMhaDbReplicationIds().removeAll(existMhaDbReplicationIds);
        }

        List<DbReplicationRouteMappingTbl> toInsertRouteMappings = routeDto.getMhaDbReplicationIds().stream().map(mhaDbReplicationId -> {
            DbReplicationRouteMappingTbl toInsertRouteMapping = new DbReplicationRouteMappingTbl();
            toInsertRouteMapping.setMhaDbReplicationId(mhaDbReplicationId);
            toInsertRouteMapping.setRouteId(routeDto.getRouteId());
            return toInsertRouteMapping;
        }).collect(Collectors.toList());
        if (!CollectionUtils.isEmpty(toInsertRouteMappings)) {
            routeMappingTblDao.insert(toInsertRouteMappings);
        }
    }

    @Override
    public void submitMhaRoutes(MhaRouteMappingDto routeMappingDto) throws SQLException {
        if (CollectionUtils.isEmpty(routeMappingDto.getRouteIds())) {
            throw ConsoleExceptionUtils.message("routeIds is empty!");
        }

        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(routeMappingDto.getMhaName(), BooleanEnum.FALSE.getCode());
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message("mha not exist!");
        }

        List<RouteTbl> routeTbls = routeTblDao.queryAllExist().stream().filter(e -> routeMappingDto.getRouteIds().contains(e.getId())).toList();
        List<RouteTbl> globalActiveRoutes = routeTbls.stream().filter(e -> e.getGlobalActive().equals(BooleanEnum.TRUE.getCode())).toList();
        if (!CollectionUtils.isEmpty(globalActiveRoutes)) {
            throw ConsoleExceptionUtils.message("exist routes are globalActive!");
        }

        List<Long> existRouteIds = routeMappingTblDao.queryByMhaId(mhaTblV2.getId()).stream().map(DbReplicationRouteMappingTbl::getRouteId).toList();
        if (!CollectionUtils.isEmpty(existRouteIds)) {
            routeMappingDto.getRouteIds().removeAll(existRouteIds);
        }
        if (!CollectionUtils.isEmpty(routeMappingDto.getRouteIds())) {
            List<DbReplicationRouteMappingTbl> insertRouteMappingTbls = routeMappingDto.getRouteIds().stream().map(routeId -> {
                DbReplicationRouteMappingTbl routeMappingTbl = new DbReplicationRouteMappingTbl();
                routeMappingTbl.setRouteId(routeId);
                routeMappingTbl.setMhaId(mhaTblV2.getId());
                return routeMappingTbl;
            }).collect(Collectors.toList());
            routeMappingTblDao.insert(insertRouteMappingTbls);
        }
    }

    @Override
    public void deleteMhaDbReplicationRoutes(MhaDbReplicationRouteDto routeDto) throws SQLException {
        List<DbReplicationRouteMappingTbl> routeMappingTbls = routeMappingTblDao.queryByMhaDbReplicationIds(routeDto.getRouteId(), routeDto.getMhaDbReplicationIds());
        if (CollectionUtils.isEmpty(routeMappingTbls)) {
            return;
        }
        routeMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        routeMappingTblDao.update(routeMappingTbls);
    }

    @Override
    public void deleteMhaRoutes(MhaDbReplicationRouteDto routeDto) throws SQLException {
        List<DbReplicationRouteMappingTbl> routeMappingTbls = routeMappingTblDao.queryByMhaIds(routeDto.getRouteId(), routeDto.getMhaIds());
        if (CollectionUtils.isEmpty(routeMappingTbls)) {
            return;
        }
        routeMappingTbls.forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        routeMappingTblDao.update(routeMappingTbls);
    }

    @Override
    public List<RouteMappingDto> getRouteMappings(String srcMhaName, String dstMhaName) throws SQLException {
        List<MhaDbReplicationDto> replicationDtos = mhaDbReplicationService.queryByMha(srcMhaName, dstMhaName, null);
        List<Long> mhaDbReplicationIds = replicationDtos.stream().map(MhaDbReplicationDto::getId).collect(Collectors.toList());
        List<DbReplicationRouteMappingTbl> dbReplicationRouteMappingTbls = routeMappingTblDao.queryByMhaDbReplicationIds(mhaDbReplicationIds);
        List<RouteTbl> routeTbls = routeTblDao.queryAllExist();
        Map<Long, Long> mhaDbReplicationId2RouteId = dbReplicationRouteMappingTbls.stream().collect(Collectors.toMap(DbReplicationRouteMappingTbl::getMhaDbReplicationId, DbReplicationRouteMappingTbl::getRouteId));
        Map<Long, RouteTbl> routeTblMap = routeTbls.stream().collect(Collectors.toMap(RouteTbl::getId, r -> r));

        Triple<Map<Long, String>, Map<Long, String>, Map<Long, String>> baseMap = getBaseMap();
        return replicationDtos.stream().map(source -> {
            RouteMappingDto target = new RouteMappingDto();
            target.setMhaDbReplicationId(source.getId());
            target.setDbName(source.getSrc().getDbName());
            if (mhaDbReplicationId2RouteId.containsKey(source.getId())) {
                RouteTbl routeTbl = routeTblMap.get(mhaDbReplicationId2RouteId.get(source.getId()));
                RouteDto routeDto = getRouteDto(routeTbl, baseMap.getLeft(), baseMap.getMiddle(), baseMap.getRight());
                target.setRouteOrgName(routeDto.getRouteOrgName());
                target.setSrcDcName(routeDto.getSrcDcName());
                target.setDstDcName(routeDto.getDstDcName());
                target.setSrcProxyUris(StringUtils.join(routeDto.getSrcProxyUris(), ","));
                target.setDstProxyUris(StringUtils.join(routeDto.getDstProxyUris(), ","));
                target.setRelayProxyUris(StringUtils.join(routeDto.getRelayProxyUris(), ","));
            }
            return target;
        }).collect(Collectors.toList());
    }

    @Override
    public List<RouteMappingDto> getRouteMappings(String mhaName) throws SQLException {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        List<DbReplicationRouteMappingTbl> routeMappingTbls = routeMappingTblDao.queryByMhaId(mhaTblV2.getId());
        if (CollectionUtils.isEmpty(routeMappingTbls)) {
            return new ArrayList<>();
        }
        List<RouteTbl> routeTbls = routeTblDao.queryAllExist();
        Map<Long, RouteTbl> routeTblMap = routeTbls.stream().collect(Collectors.toMap(RouteTbl::getId, r -> r));
        Triple<Map<Long, String>, Map<Long, String>, Map<Long, String>> baseMap = getBaseMap();

        return routeMappingTbls.stream().map(routeMapping -> {
            RouteMappingDto routeMappingDto = new RouteMappingDto();
            routeMappingDto.setMhaId(mhaTblV2.getId());
            RouteTbl routeTbl = routeTblMap.get(routeMapping.getRouteId());
            RouteDto routeDto = getRouteDto(routeTbl, baseMap.getLeft(), baseMap.getMiddle(), baseMap.getRight());
            routeMappingDto.setRouteOrgName(routeDto.getRouteOrgName());
            routeMappingDto.setSrcDcName(routeDto.getSrcDcName());
            routeMappingDto.setDstDcName(routeDto.getDstDcName());
            routeMappingDto.setSrcProxyUris(StringUtils.join(routeDto.getSrcProxyUris(), ","));
            routeMappingDto.setDstProxyUris(StringUtils.join(routeDto.getDstProxyUris(), ","));
            routeMappingDto.setRelayProxyUris(StringUtils.join(routeDto.getRelayProxyUris(), ","));

            return routeMappingDto;
        }).collect(Collectors.toList());

    }


    @Override
    public List<ApplierReplicationView> getRelatedDbs(long routeId) throws SQLException {
        List<DbReplicationRouteMappingTbl> routeMappingTbls = routeMappingTblDao.queryByRouteId(routeId).stream()
                .filter(e -> e.getMhaDbReplicationId() != null && e.getMhaDbReplicationId() > 0L)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(routeMappingTbls)) {
            return new ArrayList<>();
        }
        List<Long> mhaDbReplicationIds = routeMappingTbls.stream().map(DbReplicationRouteMappingTbl::getMhaDbReplicationId).collect(Collectors.toList());
        return mhaDbReplicationService.query(mhaDbReplicationIds);
    }

    @Override
    public List<ApplierReplicationView> getRelatedMhas(long routeId) throws SQLException {
        List<DbReplicationRouteMappingTbl> routeMappingTbls = routeMappingTblDao.queryByRouteId(routeId).stream()
                .filter(e -> e.getMhaId() != null && e.getMhaId() > 0L)
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(routeMappingTbls)) {
            return new ArrayList<>();
        }

        List<Long> mhaIds = routeMappingTbls.stream().map(DbReplicationRouteMappingTbl::getMhaId).collect(Collectors.toList());
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByIds(mhaIds);
        Map<Long, String> mhaMap = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));

        return routeMappingTbls.stream().map(source -> {
            ApplierReplicationView target = new ApplierReplicationView();
            target.setSrcMhaName(mhaMap.get(source.getMhaId()));
            target.setRelatedId(source.getMhaId());
            return target;
        }).collect(Collectors.toList());
    }

    private RouteDto getRouteDto(RouteTbl routeTbl, Map<Long, String> proxyTblMap, Map<Long, String> dcMap, Map<Long, String> buMap) {
        RouteDto routeDto = new RouteDto();
        routeDto.setId(routeTbl.getId());
        routeDto.setRouteOrgName(routeTbl.getRouteOrgId() == 0L ? null : buMap.get(routeTbl.getRouteOrgId()));

        String srcName = dcMap.get(routeTbl.getSrcDcId());
        String dstName = dcMap.get(routeTbl.getDstDcId());
        routeDto.setSrcDcName(srcName);
        routeDto.setSrcRegionName(consoleConfig.getDc2regionMap().get(srcName));
        routeDto.setDstDcName(dstName);
        routeDto.setDstRegionName(consoleConfig.getDc2regionMap().get(dstName));


        String srcProxyIds = routeTbl.getSrcProxyIds();
        String optionalProxyIds = routeTbl.getOptionalProxyIds();
        String dstProxyIds = routeTbl.getDstProxyIds();
        List<String> srcProxyUris = getProxyUris(srcProxyIds, proxyTblMap);
        List<String> relayProxyUris = getProxyUris(optionalProxyIds, proxyTblMap);
        List<String> dstProxyUris = getProxyUris(dstProxyIds, proxyTblMap);

        routeDto.setSrcProxyUris(srcProxyUris);
        routeDto.setRelayProxyUris(relayProxyUris);
        routeDto.setDstProxyUris(dstProxyUris);
        routeDto.setTag(routeTbl.getTag());
        routeDto.setDeleted(routeTbl.getDeleted());
        routeDto.setGlobalActive(routeTbl.getGlobalActive());
        return routeDto;
    }

    private List<String> getProxyUris(String proxyIds, Map<Long, String> proxyTblMap) {
        List<String> proxyIps = Lists.newArrayList();
        if (StringUtils.isNotBlank(proxyIds)) {
            String[] proxyIdArr = proxyIds.split(",");
            for (String idStr : proxyIdArr) {
                Long proxyId = Long.parseLong(idStr);
                proxyIps.add(proxyTblMap.get(proxyId));
            }
        }
        return proxyIps;
    }

    private Triple<Map<Long, String>, Map<Long, String>, Map<Long, String>> getBaseMap() throws SQLException {
        List<DcTbl> dcTbls = dcTblDao.queryAllExist();
        List<BuTbl> buTbls = buTblDao.queryAllExist();
        List<ProxyTbl> proxyTbls = proxyTblDao.queryAllExist();
        Map<Long, String> dcMap = dcTbls.stream().collect(Collectors.toMap(DcTbl::getId, DcTbl::getDcName));
        Map<Long, String> buMap = buTbls.stream().collect(Collectors.toMap(BuTbl::getId, BuTbl::getBuName));
        Map<Long, String> proxyMap = proxyTbls.stream().collect(Collectors.toMap(ProxyTbl::getId, ProxyTbl::getUri));

        return Triple.of(proxyMap, dcMap, buMap);
    }
}
