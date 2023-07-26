package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaReplicationTblDao;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by dengquanliang
 * 2023/5/25 14:09
 */
@Service
public class MhaServiceV2Impl implements MhaServiceV2 {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaReplicationTblDao mhaReplicationTblDao;

    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;

    @Override
    public Map<String, MhaTblV2> queryMhaByNames(List<String> mhaNames) {

        try {
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByMhaNames(mhaNames);
            return mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getMhaName, Function.identity(), (e1, e2) -> e1));
        } catch (SQLException e) {
            logger.error("queryByMhaNames exception",e);
            throw  ConsoleExceptionUtils.message("查询 mhaTbl 失败，请重试或联系开发。错误信息：" + e.getMessage());
        }
    }

    @Override
    public Map<Long, MhaTblV2> queryMhaByIds(List<Long> mhaIds) {
        try {
            List<MhaTblV2> mhaTblV2List = mhaTblV2Dao.queryByIds(mhaIds);
            return mhaTblV2List.stream().collect(Collectors.toMap(MhaTblV2::getId, Function.identity(), (e1, e2) -> e1));
        } catch (SQLException e) {
            logger.error("queryByMhaNames exception", e);
            throw ConsoleExceptionUtils.message("查询 mhaTbl 失败，请重试或联系开发。错误信息：" + e.getMessage());
        }
    }
}
