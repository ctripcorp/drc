package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.RowsMetaFilterParam;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.service.filter.RowsMetaFilterService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.framework.drc.core.monitor.reporter.EventMonitor;
import com.ctrip.framework.foundation.Foundation;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    public List<String> getWhiteList(String metaFilterName) throws SQLException {
        eventMonitor.logEvent("ROWS.META.FILTER.QUERY", metaFilterName);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryByMetaFilterName(metaFilterName);
        if (rowsFilterMetaTbl == null) {
            logger.info("metaFilterName: {} does not exist", metaFilterName);
            return new ArrayList<>();
        }

        List<RowsFilterMetaMappingTbl> rowsFilterMetaMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        if (CollectionUtils.isEmpty(rowsFilterMetaMappings)) {
            logger.warn("metaFilterName: {} doesn't match any filter keys", metaFilterName);
            return new ArrayList<>();
        }
        List<String> filterKeys = rowsFilterMetaMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());

        QConfigQueryParam queryParam = buildQueryParam();
        QConfigDataResponse response = qConfigApiService.getQConfigData(queryParam);
        if (response == null || !response.exist() || response.getData() == null) {
            logger.info("rows filter whitelist config does not exist, metaFilterName: {}", metaFilterName);
            return new ArrayList<>();
        }

        return getWhiteList(response.getData().getData(), filterKeys.get(0));
    }

    @Override
    public boolean addWhiteList(RowsMetaFilterParam requestBody) {
        return false;
    }

    @Override
    public boolean deleteWhiteList(RowsMetaFilterParam requestBody) {
        return false;
    }

    @Override
    public boolean updateWhiteList(RowsMetaFilterParam requestBody) {
        return false;
    }

    private QConfigQueryParam buildQueryParam() {
        QConfigQueryParam queryParam = new QConfigQueryParam();
        queryParam.setToken(domainConfig.getQConfigApiConsoleToken());
        queryParam.setGroupId(Foundation.app().getAppId());
        queryParam.setDataId(CONFIG_NAME);
        queryParam.setEnv(EnvUtils.getEnv());
        queryParam.setSubEnv(domainConfig.getWhiteListTargetSubEnv().get(0));
        queryParam.setTargetGroupId(domainConfig.getWhitelistTargetGroupId());

        return queryParam;
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
}
