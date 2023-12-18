package com.ctrip.framework.drc.console.service.v2.resource.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.ProxyTblDao;
import com.ctrip.framework.drc.console.dao.RouteTblDao;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.dto.RouteDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.v2.resource.RouteService;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
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
    private DefaultConsoleConfig consoleConfig;

    private static final String NULL_STRING = "null";

    @Override
    public String submitProxyRouteConfig(RouteDto routeDto) {
        try {
            Long routeOrgId = StringUtils.isBlank(routeDto.getRouteOrgName()) ? 0L : buTblDao.queryByBuName(routeDto.getRouteOrgName()).getId();
            Long srcDcId = dcTblDao.queryByDcName(routeDto.getSrcDcName()).getId();
            Long dstDcId = dcTblDao.queryByDcName(routeDto.getDstDcName()).getId();
            List<Long> srcProxyIds = Lists.newArrayList();
            List<Long> relayProxyIds = Lists.newArrayList();
            List<Long> dstProxyIds = Lists.newArrayList();
            for (String proxyUri : routeDto.getSrcProxyUris()) {
                srcProxyIds.add(proxyTblDao.queryByUri(proxyUri).getId());
            }
            for (String proxyUri : routeDto.getRelayProxyUris()) {
                relayProxyIds.add(proxyTblDao.queryByUri(proxyUri).getId());
            }
            for (String proxyUri : routeDto.getDstProxyUris()) {
                dstProxyIds.add(proxyTblDao.queryByUri(proxyUri).getId());
            }
            routeTblDao.upsert(routeOrgId, srcDcId, dstDcId, StringUtils.join(srcProxyIds, ","),
                    StringUtils.join(relayProxyIds, ","), StringUtils.join(dstProxyIds, ","), routeDto.getTag(), routeDto.getDeleted());
            return "update proxy route succeeded";
        } catch (SQLException e) {
            logger.error("update proxy route failed, ", e);
            return "update proxy route failed";
        }
    }

    @Override
    public List<RouteDto> getRoutes(String routeOrgName, String srcDcName, String dstDcName, String tag, Integer deleted) {
        List<RouteDto> routes = Lists.newArrayList();
        try {
            Long buId = null, srcDcId = null, dstDcId = null;
            if (null != routeOrgName) {
                // ternary operator should make sure type consistent
                buId = routeOrgName.equals(NULL_STRING) ? Long.valueOf(0L) : buTblDao.queryByBuName(routeOrgName).getId();
            }
            if (null != srcDcName) {
                srcDcId = dcTblDao.queryByDcName(srcDcName).getId();
            }
            if (null != dstDcName) {
                dstDcId = dcTblDao.queryByDcName(dstDcName).getId();
            }
            final Long finalBuId = buId, finalSrcDcId = srcDcId, finalDstDcId = dstDcId;
            List<RouteTbl> routeTbls = routeTblDao.queryAllExist().stream().filter(p -> (null == routeOrgName || p.getRouteOrgId().equals(finalBuId))
                    && (null == srcDcName || p.getSrcDcId().equals(finalSrcDcId))
                    && (null == dstDcName || p.getDstDcId().equals(finalDstDcId))
                    && (null == tag || p.getTag().equalsIgnoreCase(tag)))
                    .collect(Collectors.toList());
            for (RouteTbl routeTbl : routeTbls) {
                routes.add(getRouteDto(routeTbl));
            }
        } catch (SQLException e) {
            logger.error("[metaInfo] fail get Proxy routes, ", e);
        }
        return routes;
    }

    @Override
    public ApiResult deleteRoute(String routeOrgName, String srcDcName, String dstDcName, String tag) {
        try {
            Long buId = buTblDao.queryByBuName(routeOrgName).getId();
            Long srcDcId = dcTblDao.queryByDcName(srcDcName).getId();
            Long dstDcId = dcTblDao.queryByDcName(dstDcName).getId();
            RouteTbl routeTbl = routeTblDao.queryAllExist().stream().filter(p -> p.getRouteOrgId().equals(buId)
                    && p.getSrcDcId().equals(srcDcId)
                    && p.getDstDcId().equals(dstDcId)
                    && p.getTag().equalsIgnoreCase(tag))
                    .findFirst().orElse(null);
            if (null != routeTbl) {
                routeTbl.setDeleted(BooleanEnum.TRUE.getCode());
                routeTblDao.update(routeTbl);
                return ApiResult.getSuccessInstance(String.format("Successfully delete route %s-%s, %s->%s", routeOrgName, tag, srcDcName, dstDcName));
            }
        } catch (SQLException e) {
            logger.error("Failed delete route {}-{}, {}->{}", routeOrgName, tag, srcDcName, dstDcName, e);
        }
        return ApiResult.getFailInstance(String.format("Failed delete route %s-%s, %s->%s", routeOrgName, tag, srcDcName, dstDcName));
    }

    private RouteDto getRouteDto(RouteTbl routeTbl) throws SQLException {
        RouteDto routeDto = new RouteDto();
        routeDto.setId(routeTbl.getId());
        routeDto.setRouteOrgName(routeTbl.getRouteOrgId() == 0L ? null : buTblDao.queryByPk(routeTbl.getRouteOrgId()).getBuName());

        String srcName = dcTblDao.queryByPk(routeTbl.getSrcDcId()).getDcName();
        String dstName = dcTblDao.queryByPk(routeTbl.getDstDcId()).getDcName();
        routeDto.setSrcDcName(srcName);
        routeDto.setSrcRegionName(consoleConfig.getDc2regionMap().get(srcName));
        routeDto.setDstDcName(dstName);
        routeDto.setDstRegionName(consoleConfig.getDc2regionMap().get(dstName));

        List<ProxyTbl> proxyTbls = proxyTblDao.queryAllExist().stream().collect(Collectors.toList());

        String srcProxyIds = routeTbl.getSrcProxyIds();
        String optionalProxyIds = routeTbl.getOptionalProxyIds();
        String dstProxyIds = routeTbl.getDstProxyIds();
        List<String> srcProxyUris = getProxyUris(srcProxyIds, proxyTbls);
        List<String> relayProxyUris = getProxyUris(optionalProxyIds, proxyTbls);
        List<String> dstProxyUris = getProxyUris(dstProxyIds, proxyTbls);

        routeDto.setSrcProxyUris(srcProxyUris);
        routeDto.setRelayProxyUris(relayProxyUris);
        routeDto.setDstProxyUris(dstProxyUris);
        routeDto.setTag(routeTbl.getTag());
        routeDto.setDeleted(routeTbl.getDeleted());
        return routeDto;
    }

    private List<String> getProxyUris(String proxyIds, List<ProxyTbl> proxyTbls) {
        List<String> proxyIps = Lists.newArrayList();
        if (StringUtils.isNotBlank(proxyIds)) {
            String[] proxyIdArr = proxyIds.split(",");
            for (String idStr : proxyIdArr) {
                Long proxyId = Long.parseLong(idStr);
                proxyTbls.stream().filter(p -> p.getId().equals(proxyId)).findFirst().ifPresent(proxyTbl -> proxyIps.add(proxyTbl.getUri()));
            }
        }
        return proxyIps;
    }
}
