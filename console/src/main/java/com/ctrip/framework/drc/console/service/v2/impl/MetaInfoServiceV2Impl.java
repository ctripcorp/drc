package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.BuTblDao;
import com.ctrip.framework.drc.console.dao.DcTblDao;
import com.ctrip.framework.drc.console.dao.entity.BuTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.RegionTbl;
import com.ctrip.framework.drc.console.dao.v2.RegionTblDao;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by yongnian
 * 2023/7/26 14:09
 */
@Service
public class MetaInfoServiceV2Impl implements MetaInfoServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private DcTblDao dcTblDao;

    @Autowired
    private BuTblDao buTblDao;

    @Autowired
    private RegionTblDao regionTblDao;

    @Override
    public List<BuTbl> queryAllBu() {
        try {
            return buTblDao.queryAll();
        } catch (SQLException e) {
            logger.error("queryAllBu exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<RegionTbl> queryAllRegion() {
        try {
            return regionTblDao.queryAll();
        } catch (SQLException e) {
            logger.error("queryAllRegion exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }
}
