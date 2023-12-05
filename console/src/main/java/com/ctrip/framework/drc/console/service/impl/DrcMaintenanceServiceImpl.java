package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.ProxyTblDao;
import com.ctrip.framework.drc.console.dao.RouteTblDao;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.DrcMaintenanceService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

@Service
@Deprecated
public class DrcMaintenanceServiceImpl implements DrcMaintenanceService {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private BuTblDao buTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private RouteTblDao routeTblDao;
    @Autowired
    private ProxyTblDao proxyTblDao;

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

    public ApiResult inputDc(String dc) {
        try {
            dcTblDao.upsert(dc);
            return ApiResult.getSuccessInstance(String.format("input dc %s succeeded", dc));
        } catch (SQLException e) {
            return ApiResult.getFailInstance(String.format("input dc %s failed: %s", dc, e));
        }
    }

    public ApiResult inputBu(String bu) {
        try {
            buTblDao.upsert(bu);
            return ApiResult.getSuccessInstance(String.format("input bu %s succeeded", bu));
        } catch (SQLException e) {
            return ApiResult.getFailInstance(String.format("input bu %s failed: %s", bu, e));
        }
    }

    public ApiResult inputProxy(ProxyDto proxyDto) {
        try {
            proxyTblDao.upsert(toProxyTbl(proxyDto));
            return ApiResult.getSuccessInstance(String.format("input Proxy %s succeeded", proxyDto));
        } catch (Exception e) {
            return ApiResult.getFailInstance(String.format("input Proxy %s failed: %s", proxyDto, e));
        }
    }

    public ApiResult deleteProxy(ProxyDto proxyDto) {
        try {
            ProxyTbl proxyTbl = toProxyTbl(proxyDto);
            proxyTbl.setDeleted(BooleanEnum.TRUE.getCode());
            proxyTblDao.upsert(toProxyTbl(proxyDto));
            return ApiResult.getSuccessInstance(String.format("delete Proxy %s succeeded", proxyDto));
        } catch (Exception e) {
            return ApiResult.getFailInstance(String.format("delete Proxy %s failed: %s", proxyDto, e));
        }
    }

    private ProxyTbl toProxyTbl(ProxyDto proxyDto) throws Exception {

        if(!prerequisite(proxyDto)) {
            throw ConsoleExceptionUtils.message("all args should be not null: " + this);
        }

        ProxyTbl proxyTbl = new ProxyTbl();
        Long dcId = dcTblDao.upsert(proxyDto.getDc());
        proxyTbl.setDcId(dcId);
        proxyTbl.setUri(String.format("%s://%s:%s", proxyDto.getProtocol(), proxyDto.getIp(), proxyDto.getPort()));
        proxyTbl.setActive(BooleanEnum.TRUE.getCode());
        proxyTbl.setMonitorActive(BooleanEnum.FALSE.getCode());
        proxyTbl.setDeleted(BooleanEnum.FALSE.getCode());
        return proxyTbl;
    }

    private boolean prerequisite(ProxyDto proxyDto) {
        return StringUtils.isNotBlank(proxyDto.getDc())
                && StringUtils.isNotBlank(proxyDto.getProtocol())
                && StringUtils.isNotBlank(proxyDto.getIp())
                && StringUtils.isNotBlank(proxyDto.getPort());
    }
}
