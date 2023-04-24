package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.QConfigVersionQueryParam;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigVersion;
import com.ctrip.framework.drc.console.vo.filter.QConfigVersionResponse;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
    private static final int ERROR_VERSION = -1;

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

    @Override
    public int getQConfigVersion(QConfigVersionQueryParam param) {
        String urlFormat = domainConfig.getQConfigRestApiUrl() + CONFIG_URL + "/%s/envs/%s/subenvs/%s/versions";
        String url = String.format(urlFormat, param.getTargetGroupId(), param.getTargetEnv(), param.getTargetSubEnv());
        String getUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}&targetdataids={targetdataids}";

        QConfigVersionResponse response = HttpUtils.get(getUrl, QConfigVersionResponse.class, buildQueryVersionParam(param));
        if (response == null || response.getStatus() != 0) {
            logger.error("Query QConfig Version Error, QConfigVersionQueryParam: {}", param);
            return ERROR_VERSION;
        }
        Map<String, Integer> versionMap = response.getData().stream().collect(Collectors.toMap(QConfigVersion::getDataId, QConfigVersion::getVersion, (k1, k2) -> k1));
        return versionMap.getOrDefault(param.getTargetDataId(), ERROR_VERSION);
    }

    private Map<String, String> buildQueryParam(QConfigQueryParam param) {
        HashMap<String, String> urlParams = Maps.newHashMap();
        urlParams.put("token", param.getToken());
        urlParams.put("groupid", param.getGroupId());
        urlParams.put("dataid", param.getDataId());
        urlParams.put("env", param.getEnv());
        urlParams.put("subenv", param.getSubEnv());
        urlParams.put("targetgroupid", param.getTargetGroupId());

        return urlParams;
    }

    private Map<String, String> buildQueryVersionParam(QConfigVersionQueryParam param) {
        HashMap<String, String> urlParams = Maps.newHashMap();
        urlParams.put("token", param.getToken());
        urlParams.put("operator", param.getOperator());
        urlParams.put("serverenv", param.getServerEnv());
        urlParams.put("groupid", param.getGroupId());
        urlParams.put("targetdataids", param.getTargetDataId());

        return urlParams;
    }
}
