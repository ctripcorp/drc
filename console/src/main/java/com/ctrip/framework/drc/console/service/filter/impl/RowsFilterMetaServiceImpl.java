package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.param.filter.*;
import com.ctrip.framework.drc.console.service.assistant.RowsFilterServiceAssistant;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;
import com.ctrip.framework.drc.console.vo.filter.UpdateQConfigResponse;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.drc.core.server.utils.ThreadUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/4/24 10:29
 */
@Service
public class RowsFilterMetaServiceImpl implements RowsFilterMetaService {

    private static final Logger logger = LoggerFactory.getLogger(QConfigServiceImpl.class);

    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private QConfigApiService qConfigApiService;
    @Autowired
    private RowsFilterMetaMappingTblDao rowsFilterMetaMappingTblDao;
    @Autowired
    private RowsFilterMetaTblDao rowsFilterMetaTblDao;

    private EventMonitor eventMonitor = DefaultEventMonitorHolder.getInstance();
    private final ListeningExecutorService rowsFilterExecutorService = MoreExecutors.listeningDecorator(ThreadUtils.newFixedThreadPool(5, "rowsFilterMeta"));
    private static final String CONFIG_NAME = "drc.properties";
    private static final int RETRY_TIME = 3;

    @Override
    public QConfigDataVO getWhitelist(String metaFilterName) throws Exception {
        eventMonitor.logEvent("ROWS.META.FILTER.QUERY", metaFilterName);
        QConfigDataVO qConfigDataVO = new QConfigDataVO();
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(metaFilterName);
        if (rowsFilterMetaTbl == null) {
            logger.error("MetaFilterName: {} Does not Exist, Query Whitelist Fail", metaFilterName);
            return qConfigDataVO;
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("MetaFilterName: {} Doesn't Match Any Filter Keys, Query Whitelist Fail", metaFilterName);
            return qConfigDataVO;
        }
        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubEnvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);

        return getWhiteList(metaFilterName, targetSubEnvs.get(0), filterKeys.get(0));
    }

    @Override
    public boolean addWhitelist(RowsMetaFilterParam param, String operator) throws Exception {
        eventMonitor.logEvent("ROWS.META.FILTER.ADD", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("MetaFilterName: {} Does not Exist, Add Whitelist Fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("MetaFilterName: %s Does not Exist, Add Whitelist Fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("MetaFilterName: {} Doesn't Match Any Filter Keys, Add Whitelist Fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("MetaFilterName: %s Doesn't Match Any Filter Keys, Add Whitelist Fail", param.getMetaFilterName()));
        }

        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubEnvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);
        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName(), targetSubEnvs.get(0), filterKeys.get(0));
        Map<String, String> configMap = buildAddConfigMap(filterKeys, qConfigDataVO.getWhitelist(), param.getWhitelist());

        return batchUpdateConfig(configMap, targetSubEnvs, param.getMetaFilterName(), operator, filterKeys.get(0));
    }

    @Override
    public boolean deleteWhitelist(RowsMetaFilterParam param, String operator) throws Exception {
        eventMonitor.logEvent("ROWS.META.FILTER.DELETE", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("MetaFilterName: {} Does not Exist, Delete Whitelist Fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("MetaFilterName: %s Does not Exist, Delete Whitelist Fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("MetaFilterName: {} Doesn't Match Any Filter Keys, Delete Whitelist Fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("MetaFilterName: %s Doesn't Match Any Filter Keys, Delete Whitelist Fail", param.getMetaFilterName()));
        }

        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubEnvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);
        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName(), targetSubEnvs.get(0), filterKeys.get(0));
        Map<String, String> configMap = buildDeleteConfigMap(filterKeys, qConfigDataVO.getWhitelist(), param.getWhitelist());

        return batchUpdateConfig(configMap, targetSubEnvs, param.getMetaFilterName(), operator, filterKeys.get(0));
    }

    @Override
    public boolean updateWhitelist(RowsMetaFilterParam param, String operator) throws Exception {
        eventMonitor.logEvent("ROWS.META.FILTER.UPDATE", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("MetaFilterName: {} Does not Exist, Update Whitelist Fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("MetaFilterName: %s Does not Exist, Update Whitelist Fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("MetaFilterName: {} Doesn't Match Any Filter Keys, Update Whitelist Fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("MetaFilterName: %s Doesn't Match Any Filter Keys, Update Whitelist Fail", param.getMetaFilterName()));
        }

        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubEnvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);
        Map<String, String> configMap = buildUpdateConfigMap(filterKeys, param.getWhitelist());

        return batchUpdateConfig(configMap, targetSubEnvs, param.getMetaFilterName(), operator, filterKeys.get(0));
    }

    private QConfigDataVO getWhiteList(String metaFilterName, String targetSubEnv, String filterKey) {
        QConfigDataVO qConfigDataVO = new QConfigDataVO();
        QConfigQueryParam queryParam = RowsFilterServiceAssistant.buildQueryParam(targetSubEnv, domainConfig.getQConfigApiConsoleToken(), domainConfig.getWhitelistTargetGroupId());
        QConfigDataResponse response = qConfigApiService.getQConfigData(queryParam);
        if (response == null || !response.exist() || response.getData() == null) {
            logger.warn("Config Does not Exist, MetaFilterName: {}, TargetSubEnv: {}", metaFilterName, targetSubEnv);
            return qConfigDataVO;
        }

        qConfigDataVO.setWhitelist(RowsFilterServiceAssistant.content2Whitelist(response.getData().getData(), filterKey));
        qConfigDataVO.setVersion(response.getData().getEditVersion());
        return qConfigDataVO;
    }

    private boolean batchUpdateConfig(Map<String, String> configMap, List<String> targetSubEnvs, String metaFilterName, String operator, String filterKey) {
        List<ListenableFuture<Pair<String, Integer>>> futures = Lists.newArrayListWithCapacity(targetSubEnvs.size());
        for (String subEnv : targetSubEnvs) {
            ListenableFuture<Pair<String, Integer>> future = rowsFilterExecutorService.submit(() -> updateConfig(configMap, metaFilterName, subEnv, filterKey, operator));
            futures.add(future);
        }

        boolean result = true;
        Map<String, Integer> successMap = Maps.newLinkedHashMap();
        for (ListenableFuture<Pair<String, Integer>> future : futures) {
            Pair<String, Integer> resultPair;
            try {
                resultPair = future.get(5, TimeUnit.SECONDS);
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                logger.error("BatchUpdate Config Error, {}", e);
                result = false;
                continue;
            }
            if (resultPair != null && resultPair.getRight() > 0) {
                successMap.put(resultPair.getLeft(), resultPair.getRight());
            } else {
                logger.error("BatchUpdate Whitelist Config Fail, TargetSubEnv: {}", resultPair.getLeft());
                result = false;
            }
        }

        if (!result && !successMap.isEmpty()) {  // rollback to last version
            logger.warn("BatchUpdate Whitelist Config Fail Need To Revert, SuccessMap: {}", successMap);
            if (!revertConfig(successMap)) {
                logger.error("Revert Whitelist Config Fail, configKeys: {}", configMap.keySet());
            }
        }
        return result;
    }

    private boolean revertConfig(Map<String, Integer> successMap) {
        eventMonitor.logEvent("ROWS.META.FILTER.REVERT", "revert");
        List<ListenableFuture<Pair<String, Boolean>>> futures = Lists.newArrayListWithCapacity(successMap.size());
        for (Map.Entry<String, Integer> entry : successMap.entrySet()) {
            String targetSubEnv = entry.getKey();
            int version = entry.getValue();
            ListenableFuture<Pair<String, Boolean>> future = rowsFilterExecutorService.submit(() -> revertConfig(targetSubEnv, version));
            futures.add(future);
        }
        boolean result = true;
        for (ListenableFuture<Pair<String, Boolean>> future : futures) {
            Pair<String, Boolean> resultPair = null;
            try {
                resultPair = future.get(5, TimeUnit.SECONDS);
                if (!resultPair.getRight()) {
                    eventMonitor.logEvent("ROWS.META.FILTER.REVERT.FAIL", resultPair.getLeft());
                    logger.error("Revert Whitelist Config Error, TargetSubEnv: {}", resultPair.getLeft());
                }
            } catch (ExecutionException | InterruptedException | TimeoutException e) {
                eventMonitor.logEvent("ROWS.META.FILTER.REVERT.FAIL", "revertError");
                logger.error("Revert Config Error, {}", e);
            } finally {
                if (resultPair == null || !resultPair.getRight()) {
                    result = false;
                }
            }
        }
        return result;
    }

    private Pair<String, Boolean> revertConfig(String targetSubEnv, int version) {
        QConfigRevertParam revertParam = buildRevertParam(targetSubEnv, version);
        UpdateQConfigResponse response = qConfigApiService.revertConfig(revertParam);
        if (response != null && response.getStatus() == 0) {
            return Pair.of(targetSubEnv, true);
        }
        return Pair.of(targetSubEnv, false);
    }

    private Pair<String, Integer> updateConfig(Map<String, String> configMap, String metaFilterName, String targetSubEnv, String filterKey, String operator) {
        for (int i = 0; i < RETRY_TIME; i++) {
            QConfigDataVO qConfigDataVO = getWhiteList(metaFilterName, targetSubEnv, filterKey);
            QConfigBatchUpdateParam batchUpdateParam = buildBatchUpdateParam(configMap, targetSubEnv, operator, qConfigDataVO.getVersion());
            UpdateQConfigResponse response = qConfigApiService.batchUpdateConfig(batchUpdateParam);
            if (response != null && response.getStatus() == 0) {
                return Pair.of(targetSubEnv, qConfigDataVO.getVersion() + 1);
            }
        }
        return Pair.of(targetSubEnv, -1);
    }

    private QConfigBatchUpdateParam buildBatchUpdateParam(Map<String, String> configMap, String targetSubEnv, String operator, int version) {
        QConfigBatchUpdateParam param = new QConfigBatchUpdateParam();
        param.setToken(domainConfig.getQConfigApiConsoleToken());
        param.setTargetGroupId(domainConfig.getWhitelistTargetGroupId());
        param.setTargetEnv(EnvUtils.getEnvStr());
        param.setTargetSubEnv(targetSubEnv);
        param.setTargetDataId(CONFIG_NAME);
        param.setServerEnv(EnvUtils.getEnvStr());
        param.setGroupId(Foundation.app().getAppId());
        param.setOperator(operator);

        QConfigBatchUpdateDetailParam detailParam = new QConfigBatchUpdateDetailParam();
        detailParam.setDataid(CONFIG_NAME);
        detailParam.setVersion(version);
        detailParam.setData(configMap);
        param.setDetailParam(detailParam);
        return param;
    }

    private QConfigRevertParam buildRevertParam(String targetSubEnv, int version) {
        QConfigRevertParam param = new QConfigRevertParam();
        param.setToken(domainConfig.getQConfigApiConsoleToken());
        param.setOperator("");
        param.setServerEnv(EnvUtils.getEnvStr());
        param.setGroupId(Foundation.app().getAppId());
        param.setTargetGroupId(domainConfig.getWhitelistTargetGroupId());
        param.setTargetEnv(EnvUtils.getEnvStr());
        param.setTargetSubEnv(targetSubEnv);
        param.setTargetDataId(CONFIG_NAME);
        param.setVersion(version);

        return param;
    }

    private Map<String, String> buildAddConfigMap(List<String> filterKeys, List<String> oldWhitelist, List<String> addWhitelist) {
        List<String> whitelist = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(oldWhitelist)) {
            whitelist.addAll(oldWhitelist);
        }

        addWhitelist.stream().filter(e -> !whitelist.contains(e)).forEach(whitelist::add);
        return buildConfigMap(whitelist, filterKeys);
    }

    private Map<String, String> buildDeleteConfigMap(List<String> filterKeys, List<String> oldWhitelist, List<String> deletedWhitelist) {
        List<String> whitelist = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(oldWhitelist)) {
            whitelist.addAll(oldWhitelist);
        }

        whitelist.removeAll(deletedWhitelist);
        return buildConfigMap(whitelist, filterKeys);
    }

    private Map<String, String> buildUpdateConfigMap(List<String> filterKeys, List<String> updateWhitelist) {
        return buildConfigMap(updateWhitelist, filterKeys);
    }

    private Map<String, String> buildConfigMap(List<String> whitelist, List<String> filterKeys) {
        String configValue = Joiner.on(",").join(whitelist);
        Map<String, String> configMap = Maps.newLinkedHashMap();
        for (String filterKey : filterKeys) {
            configMap.put(filterKey, configValue);
        }
        return configMap;
    }

    private void checkParam(RowsMetaFilterParam param, String operator) {
        if (param == null || StringUtils.isBlank(param.getMetaFilterName()) || CollectionUtils.isEmpty(param.getWhitelist())) {
            throw new IllegalArgumentException("Missing Required Parameter!");
        }
        if (StringUtils.isBlank(operator)) {
            throw new IllegalArgumentException("Require Parameter Operator!");
        }
    }
}
