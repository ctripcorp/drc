package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.config.DomainConfig;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.filter.QConfigQueryParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.service.assistant.RowsFilterServiceAssistant;
import com.ctrip.framework.drc.console.service.filter.QConfigApiService;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaMappingService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.filter.QConfigDataResponse;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMessageVO;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/4/26 11:07
 */
@Service
public class RowsFilterMetaMappingServiceImpl implements RowsFilterMetaMappingService {

    private static final Logger logger = LoggerFactory.getLogger(QConfigServiceImpl.class);

    @Autowired
    private DomainConfig domainConfig;
    @Autowired
    private RowsFilterMetaTblDao rowsFilterMetaTblDao;
    @Autowired
    private RowsFilterMetaMappingTblDao rowsFilterMetaMappingTblDao;
    @Autowired
    private QConfigApiService qConfigApiService;

    private static final String TOKEN_PREFIX = "drc.uid.filter.";

    @Override
    public boolean createMetaMessage(RowsFilterMetaMessageCreateParam param) throws Exception {
        checkCreateParam(param);
        RowsFilterMetaTbl rowsFilterMetaTbl = buildRowsFilterMetaTbl(param);
        int result = rowsFilterMetaTblDao.insert(rowsFilterMetaTbl);
        return result == 1;
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    @Override
    public boolean createOrUpdateMetaMapping(RowsFilterMetaMappingCreateParam param) throws Exception {
        checkMetaMappingCreateParam(param);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryById(param.getMetaFilterId());
        if (rowsFilterMetaTbl == null) {
            logger.error("RowsFilterMetaTbl Does not Exist, MetaFilterId: {}", param.getMetaFilterId());
            throw new IllegalArgumentException(String.format("MetaFilterId: %s Does not Exist!", param.getMetaFilterId()));
        }

        List<RowsFilterMetaMappingTbl> existMappings = rowsFilterMetaMappingTblDao.queryByMetaFilterId(rowsFilterMetaTbl.getId());
        List<RowsFilterMetaMappingTbl> deleteMappings = existMappings.stream().filter(e -> !param.getFilterKeys().contains(e.getFilterKey())).collect(Collectors.toList());
        deleteMappings.forEach(mapping -> {
            mapping.setDeleted(BooleanEnum.TRUE.getCode());
        });
        rowsFilterMetaMappingTblDao.batchUpdate(deleteMappings);

        List<String> existFilterKeys = existMappings.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList());
        param.getFilterKeys().removeAll(existFilterKeys);
        List<RowsFilterMetaMappingTbl> metaMappingTbls = param.getFilterKeys().stream().map(source -> {
            RowsFilterMetaMappingTbl target = new RowsFilterMetaMappingTbl();
            target.setMetaFilterId(param.getMetaFilterId());
            target.setFilterKey(source);
            target.setDeleted(BooleanEnum.FALSE.getCode());

            return target;
        }).collect(Collectors.toList());

        rowsFilterMetaMappingTblDao.insert(metaMappingTbls);
        return true;
    }

    @Override
    public List<RowsFilterMetaMessageVO> getMetaMessages(String metaFilterName, String mhaName) throws Exception {
        List<RowsFilterMetaMessageVO> metaMappingVOS = new ArrayList<>();

        List<Long> metaFilterIds = new ArrayList<>();

        if (StringUtils.isNotBlank(mhaName)) {
            List<RowsFilterMetaMappingTbl> metaMappingTbls = rowsFilterMetaMappingTblDao.queryByFilterKey(mhaName);
            if (CollectionUtils.isEmpty(metaMappingTbls)) {
                return metaMappingVOS;
            }
            metaFilterIds = metaMappingTbls.stream().map(RowsFilterMetaMappingTbl::getMetaFilterId).collect(Collectors.toList());
        }
        List<RowsFilterMetaTbl> rowsFilterMetaTbls = rowsFilterMetaTblDao.queryByIds(metaFilterIds, metaFilterName);
        if (CollectionUtils.isEmpty(rowsFilterMetaTbls)) {
            return metaMappingVOS;
        }

        metaMappingVOS = rowsFilterMetaTbls.stream().map(source -> {
            RowsFilterMetaMessageVO target = new RowsFilterMetaMessageVO();
            target.setMetaFilterId(source.getId());
            target.setMetaFilterName(source.getMetaFilterName());
            target.setBu(source.getBu());
            target.setOwner(source.getOwner());
            target.setTargetSubEnv(JsonUtils.fromJsonToList(source.getTargetSubenv(), String.class));
            target.setFilterType(source.getFilterType());

            return target;
        }).collect(Collectors.toList());
        return metaMappingVOS;
    }

    @Override
    public RowsFilterMetaMappingVO getMetaMappings(Long metaFilterId) throws Exception {
        RowsFilterMetaMappingVO mappingVO = new RowsFilterMetaMappingVO();
        mappingVO.setMetaFilterId(metaFilterId);

        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryByPk(metaFilterId);
        if (rowsFilterMetaTbl == null) {
            throw new IllegalArgumentException(String.format("MetaFilterId: %s Not Exist!", metaFilterId));
        }
        mappingVO.setToken(rowsFilterMetaTbl.getToken());
        mappingVO.setFilterType(rowsFilterMetaTbl.getFilterType());

        List<RowsFilterMetaMappingTbl> metaMappingTbls = rowsFilterMetaMappingTblDao.queryByMetaFilterId(metaFilterId);
        if (CollectionUtils.isEmpty(metaMappingTbls)) {
            return mappingVO;
        }
        mappingVO.setFilterKeys(metaMappingTbls.stream().map(RowsFilterMetaMappingTbl::getFilterKey).collect(Collectors.toList()));

        List<String> targetSubEnvs = JsonUtils.fromJsonToList(rowsFilterMetaTbl.getTargetSubenv(), String.class);
        QConfigQueryParam queryParam = RowsFilterServiceAssistant.buildQueryParam(targetSubEnvs.get(0), domainConfig.getQConfigApiConsoleToken(), domainConfig.getWhitelistTargetGroupId());
        QConfigDataResponse response = qConfigApiService.getQConfigData(queryParam);
        if (response == null || !response.exist() || response.getData() == null) {
            logger.warn("Config Does not Exist, MetaFilterName: {}, TargetSubEnv: {}",rowsFilterMetaTbl.getMetaFilterName(), targetSubEnvs.get(0));
            return mappingVO;
        }

        mappingVO.setFilterValue(RowsFilterServiceAssistant.getConfigValue(response.getData().getData(), mappingVO.getFilterKeys().get(0)));
        return mappingVO;
    }

    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    @Override
    public boolean deleteMetaMessage(Long metaFilterId) throws Exception {
        PreconditionUtils.checkId(metaFilterId, "MetaFilterId Requires Not Null!");
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryByPk(metaFilterId);
        if (rowsFilterMetaTbl == null || BooleanEnum.TRUE.getCode().equals(rowsFilterMetaTbl.getDeleted())) {
            logger.warn("RowsFilterMetaTbl Not Exist, MetaFilterId: {}", metaFilterId);
            return false;
        }
        rowsFilterMetaTbl.setDeleted(BooleanEnum.TRUE.getCode());
        rowsFilterMetaTblDao.update(rowsFilterMetaTbl);

        List<RowsFilterMetaMappingTbl> metaMappingTbls = rowsFilterMetaMappingTblDao.queryByMetaFilterId(metaFilterId);
        if (CollectionUtils.isEmpty(metaMappingTbls)) {
            logger.info("RowsFilterMetaMappings Not Exist, MetaFilterId: {}", metaFilterId);
            return true;
        }
        metaMappingTbls.stream().forEach(e -> e.setDeleted(BooleanEnum.TRUE.getCode()));
        rowsFilterMetaMappingTblDao.batchUpdate(metaMappingTbls);
        return true;
    }

    @Override
    public List<String> getTargetSubEnvs() {
        return domainConfig.getWhiteListTargetSubEnv();
    }

    private RowsFilterMetaTbl buildRowsFilterMetaTbl(RowsFilterMetaMessageCreateParam param) {
        RowsFilterMetaTbl rowsFilterMetaTbl = new RowsFilterMetaTbl();
        rowsFilterMetaTbl.setMetaFilterName(param.getMetaFilterName());
        rowsFilterMetaTbl.setFilterType(param.getFilterType());
        rowsFilterMetaTbl.setBu(param.getBu());
        rowsFilterMetaTbl.setOwner(param.getOwner());
        rowsFilterMetaTbl.setTargetSubenv(JsonUtils.toJson(param.getTargetSubEnv()));
        rowsFilterMetaTbl.setDeleted(BooleanEnum.FALSE.getCode());
        rowsFilterMetaTbl.setToken(createToken(param.getMetaFilterName(), param.getBu()));

        return rowsFilterMetaTbl;
    }

    private void checkCreateParam(RowsFilterMetaMessageCreateParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getMetaFilterName(), "MetaFilterName Requires Not Empty!");
        PreconditionUtils.checkCollection(param.getTargetSubEnv(), "TargetSubEnv Requires Not Empty!");
        PreconditionUtils.checkString(param.getBu(), "Bu Requires Not Empty!");
        PreconditionUtils.checkString(param.getOwner(), "Owner Requires Not Empty!");
        PreconditionUtils.checkNotNull(param.getFilterType(), "FilterType Requires Not Null!");
    }

    private void checkMetaMappingCreateParam(RowsFilterMetaMappingCreateParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkId(param.getMetaFilterId(), "MetaFilterId Requires Not Null!");
        PreconditionUtils.checkCollection(param.getFilterKeys(), "FilterKeys Requires Not Empty!");
    }

    private String createToken(String metaFilterName, String bu) {
        String tokenStr = TOKEN_PREFIX + bu + "." + metaFilterName + "." + EnvUtils.getEnv();
        return DigestUtils.md5DigestAsHex(tokenStr.getBytes());
    }

}
