package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.PageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by dengquanliang
 * 2023/5/25 14:09
 */
@Service
public class MhaReplicationServiceV2Impl implements MhaReplicationServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;


    @Override
    public PageResult<MhaReplicationTbl> queryByPage(MhaReplicationQuery query) {
        try {
            List<MhaReplicationTbl> data = mhaReplicationTblDao.queryByPage(query);
            int count = mhaReplicationTblDao.count(query);
            return PageResult.newInstance(data, query.getPageIndex(), query.getPageSize(), count);
        } catch (SQLException e) {
            logger.error("queryByPage error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<MhaReplicationTbl> queryRelatedReplications(List<Long> relatedMhaId) {
        try {
            return mhaReplicationTblDao.queryByRelatedMhaId(relatedMhaId);
        } catch (SQLException e) {
            logger.error("queryRelatedReplications error", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }
}
