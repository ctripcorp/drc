package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateParam;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.QConfigRevertParam;
import com.ctrip.framework.drc.console.param.filter.QConfigVersionQueryParam;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigVersionResponse;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;
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
    private static final String PROPERTY_URL = "/properties";

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

        QConfigDataResponse response = HttpUtils.get(url, QConfigDataResponse.class, buildQueryParamMap(param));
        return response;
    }

    @Override
    public QConfigVersionResponse getQConfigVersion(QConfigVersionQueryParam param) {
        String urlFormat = domainConfig.getQConfigRestApiUrl() + CONFIG_URL + "/%s/envs/%s/subenvs/%s/versions";
        String url = String.format(urlFormat, param.getTargetGroupId(), param.getTargetEnv(), param.getTargetSubEnv());
        String getUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}&targetdataids={targetdataids}";

        QConfigVersionResponse response = HttpUtils.get(getUrl, QConfigVersionResponse.class, buildQueryVersionParamMap(param));
        return response;
    }

    @Override
    public UpdateQConfigResponse batchUpdateConfig(QConfigBatchUpdateParam param) {
        String urlFormat = domainConfig.getQConfigRestApiUrl() + PROPERTY_URL + "/%s/envs/%s/subenvs/%s" + CONFIG_URL + "/%s";
        String url = String.format(urlFormat, param.getTargetGroupId(), param.getTargetEnv(), param.getTargetSubEnv(), param.getTargetDataId());
        String postUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}";

        UpdateQConfigResponse response = HttpUtils.post(postUrl, param.getDetailParam(), UpdateQConfigResponse.class, buildBatchUpdateParamMap(param));
        return response;
    }

    @Override
    public UpdateQConfigResponse revertConfig(QConfigRevertParam param) {
        String urlFormat = domainConfig.getQConfigRestApiUrl() + CONFIG_URL + "/%s/envs/%s/subenvs/%s/%s/versions/%s/revert";
        String url = String.format(urlFormat, param.getTargetGroupId(), param.getTargetEnv(), param.getTargetSubEnv(), param.getTargetDataId(), param.getVersion());
        String postUrl = url + "?token={token}&operator={operator}&serverenv={serverenv}&groupid={groupid}";

        UpdateQConfigResponse response = HttpUtils.post(postUrl, null, UpdateQConfigResponse.class, buildRevertParamMap(param));
        return response;
    }

    private Map<String, String> buildQueryParamMap(QConfigQueryParam param) {
        HashMap<String, String> urlParams = Maps.newHashMap();
        urlParams.put("token", param.getToken());
        urlParams.put("groupid", param.getGroupId());
        urlParams.put("dataid", param.getDataId());
        urlParams.put("env", param.getEnv());
        urlParams.put("subenv", param.getSubEnv());
        urlParams.put("targetgroupid", param.getTargetGroupId());

        return urlParams;
    }

    private Map<String, String> buildQueryVersionParamMap(QConfigVersionQueryParam param) {
        HashMap<String, String> urlParams = Maps.newHashMap();
        urlParams.put("token", param.getToken());
        urlParams.put("operator", param.getOperator());
        urlParams.put("serverenv", param.getServerEnv());
        urlParams.put("groupid", param.getGroupId());
        urlParams.put("targetdataids", param.getTargetDataId());

        return urlParams;
    }

    private Map<String, String> buildBatchUpdateParamMap(QConfigBatchUpdateParam param) {
        HashMap<String, String> urlParams = Maps.newHashMap();
        urlParams.put("token", param.getToken());
        urlParams.put("operator", param.getOperator());
        urlParams.put("serverenv", param.getServerEnv());
        urlParams.put("groupid", param.getGroupId());
        return urlParams;
    }

    private Map<String, String> buildRevertParamMap(QConfigRevertParam param) {
        HashMap<String, String> urlParams = Maps.newHashMap();
        urlParams.put("token", param.getToken());
        urlParams.put("operator", param.getOperator());
        urlParams.put("serverenv", param.getServerEnv());
        urlParams.put("groupid", param.getGroupId());
        return urlParams;
    }
}
