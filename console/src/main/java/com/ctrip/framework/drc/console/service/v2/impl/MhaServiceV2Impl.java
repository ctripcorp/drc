package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.ArrayList;
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
    @Autowired
    private ReplicatorGroupTblDao replicatorGroupTblDao;
    @Autowired
    private ReplicatorTblDao replicatorTblDao;
    @Autowired
    private ResourceTblDao resourceTblDao;
    @Autowired
    private DcTblDao dcTblDao;
    @Autowired
    private MessengerGroupTblDao messengerGroupTblDao;
    @Autowired
    private MessengerTblDao messengerTblDao;

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
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }

    @Override
    public List<String> getMhaReplicators(String mhaName) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        if (mhaTblV2 == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }
        ReplicatorGroupTbl replicatorGroupTbl = replicatorGroupTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
        if (replicatorGroupTbl == null) {
            logger.info("replicatorGroupTbl not exist, mhaName: {}", mhaName);
            return new ArrayList<>();
        }
        List<ReplicatorTbl> replicatorTbls = replicatorTblDao.queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(replicatorTbls)) {
            return new ArrayList<>();
        }
        List<Long> resourceIds = replicatorTbls.stream().map(ReplicatorTbl::getResourceId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByIds(resourceIds);
        List<String> replicatorIps = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
        return replicatorIps;
    }

    @Override
    public List<String> getMhaAvailableResource(String mhaName, int type) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
        if (mhaTblV2 == null) {
            logger.info("mha: {} not exist", mhaName);
            return new ArrayList<>();
        }

        if (type != ModuleEnum.REPLICATOR.getCode() && type != ModuleEnum.APPLIER.getCode()) {
            logger.info("resource type: {} can only be replicator or applier", type);
            return new ArrayList<>();
        }
        DcTbl dcTbl = dcTblDao.queryById(mhaTblV2.getDcId());
        List<Long> dcIds = dcTblDao.queryByRegionName(dcTbl.getRegionName()).stream().map(DcTbl::getId).collect(Collectors.toList());
        List<ResourceTbl> resourceTbls = resourceTblDao.queryByDcAndType(dcIds, type);
        List<String> ips = resourceTbls.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
        return ips;
    }

    @Override
    public List<String> getMhaMessengers(String mhaName) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
            if (mhaTblV2 == null) {
                logger.info("mha: {} not exist", mhaName);
                return Collections.emptyList();
            }
            MessengerGroupTbl messengerGroupTbl = messengerGroupTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
            List<MessengerTbl> messengerTbls = messengerTblDao.queryByGroupId(messengerGroupTbl.getId());

            List<Long> resourceIds = messengerTbls.stream().map(MessengerTbl::getResourceId).collect(Collectors.toList());
            List<ResourceTbl> resourceTbl = resourceTblDao.queryByIds(resourceIds);
            List<String> ips = resourceTbl.stream().map(ResourceTbl::getIp).collect(Collectors.toList());
            return ips;
        } catch (SQLException e) {
            logger.error("getMhaAvailableMessengerResource exception", e);
            throw ConsoleExceptionUtils.message(ReadableErrorDefEnum.QUERY_TBL_EXCEPTION, e);
        }
    }
}
