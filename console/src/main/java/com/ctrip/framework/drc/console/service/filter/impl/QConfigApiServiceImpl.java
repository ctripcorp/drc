package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengquanliang
 * 2023/4/24 14:40
 */
@Service
public class QConfigApiServiceImpl implements QConfigApiService {

    private static final Logger logger = LoggerFactory.getLogger(QConfigServiceImpl.class);

    @Autowired
    private DomainConfig domainConfig;

    private EventMonitor eventMonitor = DefaultEventMonitorHolder.getInstance();
    private static final String CONFIG_URL = "/configs";

    @Override
    public QConfigDataResponse getQConfigData(QConfigQueryParam param) {
        eventMonitor.logEvent("QCONFIG.CONSOLE.QUERY", param.getDataId());
        String url = domainConfig.getQConfigRestApiUrl() + CONFIG_URL
                + "?token={token}" +
                "&groupid={groupid}" +
                "&dataid={dataid}" +
                "&env={env}" +
                "&subenv={subenv}" +
                "&targetgroupid={targetgroupid}";

        QConfigDataResponse response = HttpUtils.get(url, QConfigDataResponse.class, buildQueryParam(param));
        return response;
    }

    private Map<String, Object> buildQueryParam(QConfigQueryParam param) {
        HashMap<String, Object> urlParams = Maps.newHashMap();
        urlParams.put("token", param.getToken());
        urlParams.put("groupid", param.getGroupId());
        urlParams.put("dataid", param.getDataId());
        urlParams.put("env", param.getEnv());
        urlParams.put("subenv", param.getSubEnv());
        urlParams.put("targetgroupid", param.getTargetGroupId());

        return urlParams;
    }
}
