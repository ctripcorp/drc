package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by yongnian
 * 2023/7/26 14:09
 */
@Service
public class MhaServiceV2Impl implements MhaServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;

    @Autowired
    private MetaInfoServiceV2 metaInfoServiceV2;

    @Override
    @DalTransactional(logicDbName = "fxdrcmetadb_w")
    public Map<Long, MhaTblV2> query(String containMhaName, Long buId, Long regionId) {
        try {
            MhaQuery mhaQuery = new MhaQuery();
            mhaQuery.setContainMhaName(containMhaName);
            mhaQuery.setBuId(buId);
            if (regionId != null && regionId > 0) {
                List<DcDo> dcDos = metaInfoServiceV2.queryAllDcWithCache();
                List<Long> dcIdList = dcDos.stream()
                        .filter(e -> regionId.equals(e.getRegionId()))
                        .map(DcDo::getDcId)
                        .collect(Collectors.toList());
                if (CollectionUtils.isEmpty(dcIdList)) {
                    return Collections.emptyMap();
                }
                mhaQuery.setDcIdList(dcIdList);
            }

            if (mhaQuery.emptyQueryCondition()) {
                return Collections.emptyMap();
            }
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.query(mhaQuery);
            return mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity(), (e1, e2) -> e1));
        } catch (SQLException e) {
            logger.error("queryMhaByName exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public Map<Long, MhaTblV2> queryMhaByIds(List<Long> mhaIds) {
        if (CollectionUtils.isEmpty(mhaIds)) {
            return Collections.emptyMap();
        }
        try {
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(mhaIds);
            return mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity(), (e1, e2) -> e1));
        } catch (SQLException e) {
            logger.error("queryByMhaNames exception", e);
            throw ConsoleExceptionUtils.message("查询 mhaTbl 失败，请重试或联系开发。错误信息：" + e.getMessage());
        }
    }
}
