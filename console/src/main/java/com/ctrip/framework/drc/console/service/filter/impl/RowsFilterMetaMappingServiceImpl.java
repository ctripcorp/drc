package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.dao.RowsFilterMetaMappingTblDao;
import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaMappingTbl;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.FilterTypeEnum;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMappingCreateParam;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaMappingService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.utils.EnvUtils;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.console.vo.filter.RowsFilterMetaMappingVO;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/4/26 11:07
 */
@Service
public class RowsFilterMetaMappingServiceImpl implements RowsFilterMetaMappingService {

    private static final Logger logger = LoggerFactory.getLogger(QConfigServiceImpl.class);

    @Autowired
    private RowsFilterMetaTblDao rowsFilterMetaTblDao;
    @Autowired
    private RowsFilterMetaMappingTblDao rowsFilterMetaMappingTblDao;

    private static final String TOKEN_PREFIX = "drc.uid.filter.";

    @Override
    public boolean createMetaMessage(RowsFilterMetaMessageCreateParam param) throws SQLException {
        checkCreateParam(param);
        RowsFilterMetaTbl rowsFilterMetaTbl = buildRowsFilterMetaTbl(param);
        int result = rowsFilterMetaTblDao.insert(rowsFilterMetaTbl);
        return result == 1;
    }

    @Override
    public boolean createMetaMapping(RowsFilterMetaMappingCreateParam param) throws SQLException {
        checkMetaMappingCreateParam(param);
        RowsFilterMetaTbl rowsFilterMetaTbl = rowsFilterMetaTblDao.queryById(param.getMetaFilterId());
        if (rowsFilterMetaTbl == null) {
            logger.error("rowsFilterMetaTbl does not exist, metaFilterId: {}", param.getMetaFilterId());
            throw new IllegalArgumentException(String.format("metaFilterId: %s does not exist!", param.getMetaFilterId()));
        }

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
    public List<RowsFilterMetaMappingVO> getMetaMappings(String metaFilterName) throws SQLException {
        List<RowsFilterMetaMappingVO> metaMappingVOS = new ArrayList<>();
        List<RowsFilterMetaTbl> rowsFilterMetaTbls = rowsFilterMetaTblDao.queryByMetaFilterNames(metaFilterName);
        if (CollectionUtils.isEmpty(rowsFilterMetaTbls)) {
            return metaMappingVOS;
        }

        List<Long> metaTblIds = rowsFilterMetaTbls.stream().map(RowsFilterMetaTbl::getId).collect(Collectors.toList());
        List<RowsFilterMetaMappingTbl> metaMappingTbls = rowsFilterMetaMappingTblDao.queryByMetaFilterIdS(metaTblIds);
        Map<Long, List<String>> metaMappingMap = metaMappingTbls.stream().collect(Collectors.groupingBy(
                RowsFilterMetaMappingTbl::getMetaFilterId, Collectors.mapping(RowsFilterMetaMappingTbl::getFilterKey, Collectors.toList())));

        metaMappingVOS = rowsFilterMetaTbls.stream().map(source -> {
            RowsFilterMetaMappingVO target = new RowsFilterMetaMappingVO();
            target.setMetaFilterId(source.getId());
            target.setMetaFilterName(source.getMetaFilterName());
            target.setBu(source.getBu());
            target.setOwner(source.getOwner());
            target.setTargetSubenv(JsonUtils.fromJsonToList(source.getTargetSubenv(), String.class));
            target.setFilterType(FilterTypeEnum.getDescByCode(source.getFilterType()));
            target.setToken(source.getToken());
            target.setFilterKeys(metaMappingMap.getOrDefault(source.getId(), new ArrayList<>()));

            return target;
        }).collect(Collectors.toList());
        return metaMappingVOS;
    }

    private RowsFilterMetaTbl buildRowsFilterMetaTbl(RowsFilterMetaMessageCreateParam param) {
        RowsFilterMetaTbl rowsFilterMetaTbl = new RowsFilterMetaTbl();
        rowsFilterMetaTbl.setMetaFilterName(param.getMetaFilterName());
        rowsFilterMetaTbl.setFilterType(param.getFilterType());
        rowsFilterMetaTbl.setBu(param.getBu());
        rowsFilterMetaTbl.setOwner(param.getOwner());
        rowsFilterMetaTbl.setTargetSubenv(JsonUtils.toJson(param.getTargetSubenv()));
        rowsFilterMetaTbl.setDeleted(BooleanEnum.FALSE.getCode());
        rowsFilterMetaTbl.setToken(createToken(param.getMetaFilterName(), param.getBu()));

        return rowsFilterMetaTbl;
    }

    private void checkCreateParam(RowsFilterMetaMessageCreateParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getMetaFilterName(), "metaFilterName requires not empty!");
        PreconditionUtils.checkCollection(param.getTargetSubenv(), "targetSubenv requires not empty!");
        PreconditionUtils.checkString(param.getBu(), "bu requires not empty!");
        PreconditionUtils.checkString(param.getOwner(), "owner requires not empty!");
        Preconditions.checkNotNull(param.getFilterType(), "filterType requires not null!");
    }

    private void checkMetaMappingCreateParam(RowsFilterMetaMappingCreateParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkId(param.getMetaFilterId(), "metaFilterId requires not null!");
        PreconditionUtils.checkCollection(param.getFilterKeys(), "filterKeys requires not empty!");
    }

    private String createToken(String metaFilterName, String bu) {
        String tokenStr = TOKEN_PREFIX + bu + "." + metaFilterName + "." + EnvUtils.getEnv();
        return DigestUtils.md5DigestAsHex(tokenStr.getBytes());
    }

}
