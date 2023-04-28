package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateDetailParam;
import com.ctrip.framework.drc.console.param.filter.QConfigBatchUpdateParam;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.thread.ConsoleThreadFactory;
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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
    private static ExecutorService EXECUTOR_SERVICE = ConsoleThreadFactory.rowsFilterMetaExecutor();
    private static final String CONFIG_NAME = "drc.properties";
    private static final int CONFIG_SPLIT_LENGTH = 2;
    private static final int ERROR_STATUS = -1;
    private static final int RETRY_TIME = 3;

    @Override
    public QConfigDataVO getWhiteList(String metaFilterName) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.QUERY", metaFilterName);
        QConfigDataVO qConfigDataVO = new QConfigDataVO();
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(metaFilterName);
        if (rowsFilterMetaTbl == null) {
            logger.error("metaFilterName: {} does not exist, query whitelist fail", metaFilterName);
            return qConfigDataVO;
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("metaFilterName: {} doesn't match any filter keys, query whitelist fail", metaFilterName);
            return qConfigDataVO;
        }
        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubenvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);

        return getWhiteList(metaFilterName, targetSubenvs.get(0), filterKeys.get(0));
    }

    @Override
    public boolean addWhiteList(RowsMetaFilterParam param, String operator) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.ADD", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("metaFilterName: {} does not exist, add whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s does not exist, add whitelist fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("metaFilterName: {} doesn't match any filter keys, add whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s doesn't match any filter keys add whitelist fail", param.getMetaFilterName()));
        }

        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubenvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);
        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName(), targetSubenvs.get(0), filterKeys.get(0));
        Map<String, String> configMap = buildAddConfigMap(filterKeys, qConfigDataVO.getWhitelist(), param.getWhitelist());

        return batchUpdateConfig(configMap, targetSubenvs, param.getMetaFilterName(), operator, filterKeys.get(0));
    }

    @Override
    public boolean deleteWhiteList(RowsMetaFilterParam param, String operator) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.DELETE", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("metaFilterName: {} does not exist, delete whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s does not exist, delete whitelist fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("metaFilterName: {} doesn't match any filter keys, delete whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s doesn't match any filter keys, delete whitelist fail", param.getMetaFilterName()));
        }

        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubenvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);
        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName(), targetSubenvs.get(0), filterKeys.get(0));
        Map<String, String> configMap = buildDeleteConfigMap(filterKeys, qConfigDataVO.getWhitelist(), param.getWhitelist());

        return batchUpdateConfig(configMap, targetSubenvs, param.getMetaFilterName(), operator, filterKeys.get(0));
    }

    @Override
    public boolean updateWhiteList(RowsMetaFilterParam param, String operator) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.UPDATE", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryOneByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("metaFilterName: {} does not exist, update whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s does not exist, delete whitelist fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("metaFilterName: {} doesn't match any filter keys, update whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s doesn't match any filter keys, update whitelist fail", param.getMetaFilterName()));
        }

        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        List<String> targetSubenvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);
        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName(), targetSubenvs.get(0), filterKeys.get(0));
        Map<String, String> configMap = buildUpdateConfigMap(filterKeys, param.getWhitelist());

        return batchUpdateConfig(configMap, targetSubenvs, param.getMetaFilterName(), operator, filterKeys.get(0));
    }

    private QConfigDataVO getWhiteList(String metaFilterName, String targetEnv, String filterKey) throws SQLException {
        QConfigDataVO qConfigDataVO = new QConfigDataVO();
        QConfigQueryParam queryParam = buildQueryParam(targetEnv);
        QConfigDataResponse response = qConfigApiService.getQConfigData(queryParam);
        if (response == null || !response.exist() || response.getData() == null) {
            logger.info("rows filter whitelist config does not exist, metaFilterName: {}", metaFilterName);
            return qConfigDataVO;
        }

        qConfigDataVO.setWhitelist(content2Whitelist(response.getData().getData(), filterKey));
        qConfigDataVO.setVersion(response.getData().getEditVersion());
        return qConfigDataVO;
    }

    private boolean batchUpdateConfig(Map<String, String> configMap, List<String> targetSubenvs, String metaFilterName, String operator, String filterKey) throws SQLException {
        ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(EXECUTOR_SERVICE);
        List<ListenableFuture<Pair<String, Integer>>> futures = Lists.newArrayListWithCapacity(targetSubenvs.size());
        for (String subenv : targetSubenvs) {
            ListenableFuture<Pair<String, Integer>> future = listeningExecutorService.submit(() -> batchUpdateConfig(configMap, metaFilterName, subenv, filterKey, operator));
            futures.add(future);
        }

        boolean result = true;
        Map<String, Integer> successMap = Maps.newLinkedHashMap();
        for (ListenableFuture<Pair<String, Integer>> future : futures) {
            Pair<String, Integer> resultPair;
            try {
                resultPair = future.get();
            } catch (ExecutionException | InterruptedException e) {
                logger.error("batchUpdate config error, {}", e);
                throw new RuntimeException(e);
            }
            if (resultPair != null && resultPair.getRight() > 0) {
                successMap.put(resultPair.getLeft(), resultPair.getRight());
            } else {
                result = false;
                break;
            }
        }

        return false;

//        if (MapUtils.isNotEmpty(successMap)) {  // rollback to last version

//        }
    }

//    private boolean
    private Pair<String, Integer> batchUpdateConfig(Map<String, String> configMap, String metaFilterName, String targetSubenv, String filterKey, String operator) throws SQLException {
        for (int i = 0; i < RETRY_TIME; i++) {
            QConfigDataVO qConfigDataVO = getWhiteList(metaFilterName, targetSubenv, filterKey);
            QConfigBatchUpdateParam batchUpdateParam = buildBatchUpdateParam(configMap, targetSubenv, operator, qConfigDataVO.getVersion());
            UpdateQConfigResponse response = qConfigApiService.batchUpdateConfig(batchUpdateParam);
            if (response != null && response.getStatus() == 0) {
                return Pair.of(targetSubenv, qConfigDataVO.getVersion() + 1);
            }
        }
        return Pair.of(targetSubenv, -1);
    }

    private QConfigQueryParam buildQueryParam(String subenv) {
        QConfigQueryParam queryParam = new QConfigQueryParam();
        queryParam.setToken(domainConfig.getQConfigApiConsoleToken());
        queryParam.setGroupId(Foundation.app().getAppId());
        queryParam.setDataId(CONFIG_NAME);
        queryParam.setEnv(EnvUtils.getEnv());
        queryParam.setSubEnv(subenv);
        queryParam.setTargetGroupId(domainConfig.getWhitelistTargetGroupId());

        return queryParam;
    }

    private QConfigBatchUpdateParam buildBatchUpdateParam(Map<String, String> configMap, String subenv, String operator, int version) {
        QConfigBatchUpdateParam param = new QConfigBatchUpdateParam();
        param.setToken(domainConfig.getQConfigApiConsoleToken());
        param.setTargetGroupId(domainConfig.getWhitelistTargetGroupId());
        param.setTargetEnv(EnvUtils.getEnv());
        param.setTargetSubEnv(subenv);
        param.setTargetDataId(CONFIG_NAME);
        param.setServerEnv(EnvUtils.getEnv());
        param.setGroupId(Foundation.app().getAppId());
        param.setOperator(operator);

        QConfigBatchUpdateDetailParam detailParam = new QConfigBatchUpdateDetailParam();
        detailParam.setDataid(CONFIG_NAME);
        detailParam.setVersion(version);
        detailParam.setData(configMap);
        param.setDetailParam(detailParam);
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

    private List<String> content2Whitelist(String content, String key) {
        Map<String, String> configMap = string2Map(content);
        String configValue = configMap.getOrDefault(key, "");
        if (StringUtils.isBlank(configValue)) {
            return new ArrayList<>();
        }
        return Arrays.stream(configValue.split(",")).collect(Collectors.toList());
    }

    private Map<String, String> string2Map(String content) {
        Map<String, String> configMap = Maps.newLinkedHashMap();
        if (StringUtils.isEmpty(content)) {
            return configMap;
        }
        String[] configs = content.split("\n");
        for (int i = 0; i < configs.length; i++) {
            String[] entry = configs[i].split("=");
            if (entry.length == CONFIG_SPLIT_LENGTH) {
                configMap.put(entry[0], entry[1]);
            }
        }
        return configMap;
    }

    private void checkParam(RowsMetaFilterParam param, String operator) {
        if (param == null || StringUtils.isBlank(param.getMetaFilterName()) || CollectionUtils.isEmpty(param.getWhitelist())) {
            throw new IllegalArgumentException("missing required parameter!");
        }
        if (StringUtils.isBlank(operator)) {
            throw new IllegalArgumentException("require parameter operator!");
        }
    }
}
