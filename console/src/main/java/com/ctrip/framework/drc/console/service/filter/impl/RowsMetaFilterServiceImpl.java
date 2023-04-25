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
import com.ctrip.framework.drc.console.service.filter.RowsMetaFilterService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataVO;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/4/24 10:29
 */
@Service
public class RowsMetaFilterServiceImpl implements RowsMetaFilterService {

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
    private static final String CONFIG_NAME = "drc.properties";
    private static final int CONFIG_SPLIT_LENGTH = 2;

    @Override
    public QConfigDataVO getWhiteList(String metaFilterName) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.QUERY", metaFilterName);

        QConfigDataVO qConfigDataVO = new QConfigDataVO();
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryByMetaFilterName(metaFilterName);
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

        List<String> desRegions = Arrays.stream(rowsFilterMetaTbl.getDesRegion().split(",")).collect(Collectors.toList());
        QConfigQueryParam queryParam = buildQueryParam(desRegions.get(0));
        QConfigDataResponse response = qConfigApiService.getQConfigData(queryParam);
        if (response == null || !response.exist() || response.getData() == null) {
            logger.info("rows filter whitelist config does not exist, metaFilterName: {}", metaFilterName);
            return qConfigDataVO;
        }

        qConfigDataVO.setWhitelist(getWhiteList(response.getData().getData(), filterKeys.get(0)));
        qConfigDataVO.setVersion(response.getData().getEditVersion());
        return qConfigDataVO;
    }

    @Override
    public boolean addWhiteList(RowsMetaFilterParam param, String operator) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.ADD", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("metaFilterName: {} does not exist, add whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s does not exist, add whitelist fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("metaFilterName: {} doesn't match any filter keys, add whitelist fail", param.getWhiteList());
            throw new IllegalArgumentException(String.format("metaFilterName: {} does not exist, add whitelist fail", param.getWhiteList()));
        }
        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());

        List<String> desRegions = Arrays.stream(rowsFilterMetaTbl.getDesRegion().split(",")).collect(Collectors.toList());
        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName());
        Map<String, String> configMap = buildAddConfigMap(filterKeys, qConfigDataVO.getWhitelist(), param.getWhiteList());

        for (String desRegion : desRegions) {
            QConfigBatchUpdateParam batchUpdateParam = buildBatchUpdateParam(configMap, desRegion, operator, qConfigDataVO.getVersion());
            qConfigApiService.batchUpdateConfig(batchUpdateParam);
        }

        return true;
    }

    @Override
    public boolean deleteWhiteList(RowsMetaFilterParam param, String operator) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.DELETE", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("metaFilterName: {} does not exist, delete whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s does not exist, delete whitelist fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("metaFilterName: {} doesn't match any filter keys, delete whitelist fail", param.getWhiteList());
            throw new IllegalArgumentException(String.format("metaFilterName: {} does not exist, delete whitelist fail", param.getWhiteList()));
        }
        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());

        List<String> desRegions = Arrays.stream(rowsFilterMetaTbl.getDesRegion().split(",")).collect(Collectors.toList());
        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName());
        Map<String, String> configMap = buildDeleteConfigMap(filterKeys, qConfigDataVO.getWhitelist(), param.getWhiteList());

        for (String desRegion : desRegions) {
            QConfigBatchUpdateParam batchUpdateParam = buildBatchUpdateParam(configMap, desRegion, operator, qConfigDataVO.getVersion());
            qConfigApiService.batchUpdateConfig(batchUpdateParam);
        }

        return true;
    }

    @Override
    public boolean updateWhiteList(RowsMetaFilterParam param, String operator) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.UPDATE", param.getMetaFilterName());
        checkParam(param, operator);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryByMetaFilterName(param.getMetaFilterName());
        if (rowsFilterMetaTbl == null) {
            logger.error("metaFilterName: {} does not exist, update whitelist fail", param.getMetaFilterName());
            throw new IllegalArgumentException(String.format("metaFilterName: %s does not exist, delete whitelist fail", param.getMetaFilterName()));
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.error("metaFilterName: {} doesn't match any filter keys, update whitelist fail", param.getWhiteList());
            throw new IllegalArgumentException(String.format("metaFilterName: {} does not exist, update whitelist fail", param.getWhiteList()));
        }
        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());

        List<String> desRegions = Arrays.stream(rowsFilterMetaTbl.getDesRegion().split(",")).collect(Collectors.toList());

        QConfigDataVO qConfigDataVO = getWhiteList(param.getMetaFilterName());
        Map<String, String> configMap = buildUpdateConfigMap(filterKeys, param.getWhiteList());

        for (String desRegion : desRegions) {
            QConfigBatchUpdateParam batchUpdateParam = buildBatchUpdateParam(configMap, desRegion, operator, qConfigDataVO.getVersion());
            qConfigApiService.batchUpdateConfig(batchUpdateParam);
        }

        return true;
    }

    private QConfigQueryParam buildQueryParam(String desRegion) {
        QConfigQueryParam queryParam = new QConfigQueryParam();
        queryParam.setToken(domainConfig.getQConfigApiConsoleToken());
        queryParam.setGroupId(Foundation.app().getAppId());
        queryParam.setDataId(CONFIG_NAME);
        queryParam.setEnv(EnvUtils.getEnv());
        queryParam.setSubEnv(desRegion);
        queryParam.setTargetGroupId(domainConfig.getWhitelistTargetGroupId());

        return queryParam;
    }

    private QConfigBatchUpdateParam buildBatchUpdateParam(Map<String, String> configMap, String desRegion, String operator, int version) {
        QConfigBatchUpdateParam param = new QConfigBatchUpdateParam();
        param.setToken(domainConfig.getQConfigApiConsoleToken());
        param.setTargetGroupId(domainConfig.getWhitelistTargetGroupId());
        param.setTargetEnv(EnvUtils.getEnv());
        param.setTargetSubEnv(desRegion);
        param.setTargetDataId(CONFIG_NAME);
        param.setServerEnv(EnvUtils.getEnv());
        param.setGroupId(Foundation.app().getAppId());
        param.setOperator(operator);

        QConfigBatchUpdateDetailParam detailParam = new QConfigBatchUpdateDetailParam();
        detailParam.setVersion(version);
        detailParam.setConfigMap(configMap);
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

    private List<String> getWhiteList(String content, String key) {
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
        if (param == null || StringUtils.isBlank(param.getMetaFilterName()) || CollectionUtils.isEmpty(param.getWhiteList())) {
            throw new IllegalArgumentException("missing required parameter!");
        }
        if (StringUtils.isBlank(operator)) {
            throw new IllegalArgumentException("require parameter operator!");
        }
    }
}
