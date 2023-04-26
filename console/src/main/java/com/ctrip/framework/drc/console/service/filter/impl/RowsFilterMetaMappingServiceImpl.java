package com.ctrip.framework.drc.console.service.filter.impl;

import com.ctrip.framework.drc.console.dao.RowsFilterMetaTblDao;
import com.ctrip.framework.drc.console.dao.entity.RowsFilterMetaTbl;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.param.filter.RowsFilterMetaMessageCreateParam;
import com.ctrip.framework.drc.console.service.MhaService;
import com.ctrip.framework.drc.console.service.filter.RowsFilterMetaMappingService;
import com.ctrip.framework.drc.console.service.remote.qconfig.QConfigServiceImpl;
import com.ctrip.framework.drc.console.utils.PreconditionUtils;
import com.ctrip.framework.drc.core.service.utils.JsonUtils;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;

/**
 * Created by dengquanliang
 * 2023/4/26 11:07
 */
@Service
public class RowsFilterMetaMappingServiceImpl implements RowsFilterMetaMappingService {

    private static final Logger logger = LoggerFactory.getLogger(QConfigServiceImpl.class);

    @Autowired
    private MhaService mhaService;
    @Autowired
    private RowsFilterMetaTblDao rowsFilterMetaTblDao;

    private static final String FILTER_KEY_PREFIX = "uid.filter.whitelist.";

    @Override
    public boolean createMetaMessage(RowsFilterMetaMessageCreateParam param) throws SQLException {
        checkCreateParam(param);
        String srcDc = mhaService.getDcNameForMha(param.getSrcMha());
        String desDc = mhaService.getDcNameForMha(param.getDesMha());
        if (StringUtils.isBlank(srcDc) || StringUtils.isBlank(desDc)) {
            logger.error("srcMha: {} or desMha: {} can not find matching dc", param.getSrcMha(), param.getDesMha());
            throw new IllegalArgumentException("srcMha or desMha is illegal!");
        }

        RowsFilterMetaTbl rowsFilterMetaTbl = buildRowsFilterMetaTbl(param, srcDc, desDc);
        int result = rowsFilterMetaTblDao.insert(rowsFilterMetaTbl);
        return result == 1;
    }

    private RowsFilterMetaTbl buildRowsFilterMetaTbl(RowsFilterMetaMessageCreateParam param, String srcDc, String desDc) {
        RowsFilterMetaTbl rowsFilterMetaTbl = new RowsFilterMetaTbl();
        rowsFilterMetaTbl.setMetaFilterName(buildMetaFilterName(param.getClusterName(), param.getDesMha(), param.getSrcMha()));
        rowsFilterMetaTbl.setFilterType(param.getFilterType());
        rowsFilterMetaTbl.setBu(param.getBu());
        rowsFilterMetaTbl.setOwner(param.getOwner());
        rowsFilterMetaTbl.setSrcDc(srcDc);
        rowsFilterMetaTbl.setDesDC(desDc);
        rowsFilterMetaTbl.setTargetSubenv(JsonUtils.toJson(param.getTargetSubenv()));
        rowsFilterMetaTbl.setDeleted(BooleanEnum.FALSE.getCode());

        return rowsFilterMetaTbl;
    }

    private void checkCreateParam(RowsFilterMetaMessageCreateParam param) {
        PreconditionUtils.checkNotNull(param);
        PreconditionUtils.checkString(param.getSrcMha(), "srcMha requires not empty!");
        PreconditionUtils.checkString(param.getDesMha(), "desMha requires not empty!");
        PreconditionUtils.checkCollection(param.getTargetSubenv(), "srcMha requires not empty!");
        PreconditionUtils.checkString(param.getBu(), "bu requires not null!");
        PreconditionUtils.checkString(param.getOwner(), "owner requires not null!");
        Preconditions.checkNotNull(param.getFilterType(), "filterType requires not null");
    }
    
    private String buildMetaFilterName(String clusterName, String desMha, String srcMha) {
        return FILTER_KEY_PREFIX + clusterName + "." + desMha + "." + srcMha;
    }
}
