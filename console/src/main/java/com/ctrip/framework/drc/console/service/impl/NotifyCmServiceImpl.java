package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.DcTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.HttpRequestEnum;
import com.ctrip.framework.drc.console.enums.DlockEnum;
import com.ctrip.framework.drc.console.service.NotifyCmService;
import com.ctrip.framework.drc.core.config.RegionConfig;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shiruixin
 * 2025/4/16 16:31
 */
@Service
public class NotifyCmServiceImpl implements NotifyCmService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private RegionConfig regionConfig = RegionConfig.getInstance();
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;
    @Autowired
    private DcTblDao dcTblDao;

    private static final String POST_BASE_API_URL = "/api/meta/clusterchange/%s/?operator={operator}&dcId={dcId}";
    private static final String PUT_BASE_API_URL = "/api/meta/clusterchange/%s/?operator={operator}";
    @Override
    public void pushConfigToCM(List<String> mhaNames, DlockEnum operator, HttpRequestEnum httpRequestEnum) throws Exception {
        for (String mhaName : mhaNames) {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
            pushConfig(operator.getOperator(), httpRequestEnum, mhaTblV2);
        }
    }

    private void pushConfig(String operator, HttpRequestEnum httpRequestEnum, MhaTblV2 mhaTblV2) throws SQLException {
        Map<String, String> cmRegionUrls = regionConfig.getCMRegionUrls();
        DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
        String dbClusterId = mhaTblV2.getClusterName() + "." + mhaTblV2.getMhaName();
        try {
            String url = null;
            Map<String, String> paramMap = new HashMap<>();
            paramMap.put("operator", operator);
            if (httpRequestEnum.equals(HttpRequestEnum.POST)) {
                paramMap.put("dcId", dcTbl.getDcName());
                url = cmRegionUrls.get(dcTbl.getRegionName()) + String.format(POST_BASE_API_URL, dbClusterId);
                HttpUtils.post(url, null, ApiResult.class, paramMap);
            } else if (httpRequestEnum.equals(HttpRequestEnum.PUT)) {
                url = cmRegionUrls.get(dcTbl.getRegionName()) + String.format(PUT_BASE_API_URL, dbClusterId);
                HttpUtils.put(url, null, ApiResult.class, paramMap);
            }
        } catch (Exception e) {
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.console.push.config.fail", mhaTblV2.getMhaName());
            logger.error("pushConfigToCM fail: {}", mhaTblV2.getMhaName(), e);
        }
    }


    public void pushConfigToCM(List<Long> mhaIds, String operator, HttpRequestEnum httpRequestEnum) throws Exception {
        for (long mhaId : mhaIds) {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryById(mhaId);
            pushConfig(operator, httpRequestEnum, mhaTblV2);
        }
    }
}
