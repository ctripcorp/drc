package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaReplicationTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaDelayInfoDto;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.param.v2.MhaReplicationQuery;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaReplicationServiceV2;
import com.ctrip.framework.drc.console.service.v2.MysqlServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.http.PageResult;
import com.google.common.collect.Lists;
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
    private MetaInfoServiceV2 metaInfoServiceV2;
    @Autowired
    private MysqlServiceV2 mysqlServiceV2;
    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;

    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;


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


    @Override
    public MhaDelayInfoDto getMhaReplicationDelay(String srcMha, String dstMha) {
        try {
            // check
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByMhaNames(Lists.newArrayList(srcMha, dstMha));
            MhaTblV2 srcMhaTbl = mhaTblV2List.stream().filter(e -> e.getMhaName().equals(srcMha)).findAny()
                    .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not found: " + srcMha));
            MhaTblV2 dstMhaTbl = mhaTblV2List.stream().filter(e -> e.getMhaName().equals(dstMha)).findAny()
                    .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not found: " + dstMha));

            // query dst first, result could be larger than original
            long start = System.currentTimeMillis();
            Long dstTime = mysqlServiceV2.getDelayUpdateTime(dstMha, srcMha);
            logger.info("[delay query] dstMha:{}, dstTime:{}, cost:{}", dstMha, dstTime, System.currentTimeMillis() - start);

            start = System.currentTimeMillis();
            Long srcTime = mysqlServiceV2.getDelayUpdateTime(srcMha, srcMha);
            logger.info("[delay query] srcMha:{}, srcTime:{}, cost:{}", srcMha, srcTime, System.currentTimeMillis() - start);

            Long delay = null;
            if (srcTime != null && dstTime != null) {
                delay = srcTime - dstTime;
            }
            return new MhaDelayInfoDto(srcTime, dstTime, delay);
        } catch (Exception e) {
            if (e instanceof ConsoleException) {
                throw (ConsoleException) e;
            }
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public Long getMhaLastUpdateTime(String mha, String srcMha) {
        try {
            // check
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByMhaNames(Lists.newArrayList(srcMha, mha));
            MhaTblV2 srcMhaTbl = mhaTblV2List.stream().filter(e -> e.getMhaName().equals(srcMha)).findAny()
                    .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not found: " + srcMha));
            MhaTblV2 mhaTbl = mhaTblV2List.stream().filter(e -> e.getMhaName().equals(mha)).findAny()
                    .orElseThrow(() -> ConsoleExceptionUtils.message(ReadableErrorDefEnum.REQUEST_PARAM_INVALID, "mha not found: " + mha));

            // query dst first, result could be larger than original
            long start = System.currentTimeMillis();
            Long dstTime = mysqlServiceV2.getDelayUpdateTime(mha, srcMha);
            logger.info("[delay query] mha:{}, dstTime:{}, cost:{}", mha, dstTime, System.currentTimeMillis() - start);
            return dstTime;
        } catch (Exception e) {
            if (e instanceof ConsoleException) {
                throw (ConsoleException) e;
            }
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }
}
