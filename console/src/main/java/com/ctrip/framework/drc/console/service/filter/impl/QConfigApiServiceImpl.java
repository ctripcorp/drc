package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateParam;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.QConfigRevertParam;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;
import com.ctrip.framework.drc.core.http.HttpUtils;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
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

    @Autowired
    private DomainConfig domainConfig;

    private EventMonitor eventMonitor = DefaultEventMonitorHolder.getInstance();
    private static final String CONFIG_URL = "/configs";
    private static final String PROPERTY_URL = "/properties";

    @Override
    public QConfigDataResponse getQConfigData(QConfigQueryParam param) {
        eventMonitor.logEvent("QCONFIG.CONSOLE.QUERY", param.getSubEnv());
        String url = domainConfig.getQConfigRestApiUrl() + CONFIG_URL
                + "?token={token}" +
                "&groupid={groupid}" +
                "&dataid={dataid}" +
                "&env={env}" +
                "&subenv={subenv}" +
                "&targetgroupid={targetgroupid}";

        QConfigDataResponse response = HttpUtils.get(url, QConfigDataResponse.class, buildQueryParamMap(param));
        return response;
    }

    @Override
    public UpdateQConfigResponse batchUpdateConfig(QConfigBatchUpdateParam param) {
        eventMonitor.logEvent("QCONFIG.CONSOLE.UPDATE", param.getTargetSubEnv());
        String urlFormat = domainConfig.getQConfigRestApiUrl() + PROPERTY_URL + "/%s/envs/%s/subenvs/%s" + CONFIG_URL + "/%s";
        String url = String.format(urlFormat, param.getTargetGroupId(), param.getTargetEnv(), param.getTargetSubEnv(), param.getTargetDataId());
        String postUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}";

        UpdateQConfigResponse response = HttpUtils.post(postUrl, param.getDetailParam(), UpdateQConfigResponse.class, param.extractMap());
        return response;
    }

    @Override
    public UpdateQConfigResponse revertConfig(QConfigRevertParam param) {
        eventMonitor.logEvent("QCONFIG.CONSOLE.REVERT", param.getTargetSubEnv());
        String urlFormat = domainConfig.getQConfigRestApiUrl() + CONFIG_URL + "/%s/envs/%s/subenvs/%s/%s/versions/%s/revert";
        String url = String.format(urlFormat, param.getTargetGroupId(), param.getTargetEnv(), param.getTargetSubEnv(), param.getTargetDataId(), param.getVersion());
        String postUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}";

        UpdateQConfigResponse response = HttpUtils.post(postUrl, null, UpdateQConfigResponse.class, param.extractMap());
        return response;
    }

    private Map<String, String> buildQueryParamMap(QConfigQueryParam param) {
        Map<String, String> urlParams = new HashMap<>();
        urlParams.put("token", param.getToken());
        urlParams.put("groupid", param.getGroupId());
        urlParams.put("dataid", param.getDataId());
        urlParams.put("env", param.getEnv());
        urlParams.put("subenv", param.getSubEnv());
        urlParams.put("targetgroupid", param.getTargetGroupId());

        return urlParams;
    }

}
