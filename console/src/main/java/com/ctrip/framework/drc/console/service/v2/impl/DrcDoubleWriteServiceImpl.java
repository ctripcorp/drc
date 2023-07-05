package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.service.v2.DrcDoubleWriteService;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.platform.dal.dao.DalHints;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * Created by dengquanliang
 * 2023/7/5 15:01
 */
@Service
public class DrcDoubleWriteServiceImpl implements DrcDoubleWriteService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private MhaGroupTblDao mhaGroupTblDao;
    @Autowired
    private GroupMappingTblDao groupMappingTblDao;
    @Autowired
    private ClusterMhaMapTblDao clusterMhaMapTblDao;
    @Autowired
    private ClusterTblDao clusterTblDao;
    @Autowired
    private MhaTblDao mhaTblDao;
    @Autowired
    private MhaTblV2Dao mhaTblV2Dao;

    private static final int GROUP_SIZE = 2;

    @Override
    public void buildMha(Long mhaGroupId) throws Exception {
        MhaGroupTbl mhaGroup = mhaGroupTblDao.queryByPk(mhaGroupId);
        List<GroupMappingTbl> groupMappings = groupMappingTblDao.queryByMhaGroupIds(Lists.newArrayList(mhaGroupId), BooleanEnum.FALSE.getCode());
        if (CollectionUtils.isEmpty(groupMappings) || groupMappings.size() != GROUP_SIZE) {
            logger.error("groupMapping not exist or contains more than 2 mha, mhaGroupId: {}", mhaGroupId);
            throw ConsoleExceptionUtils.message("groupMapping not exist or contains more than 2 mha");
        }
        long mhaId0 = groupMappings.get(0).getMhaId();
        long mhaId1 = groupMappings.get(1).getMhaId();

        MhaTbl mha0 = mhaTblDao.queryById(mhaId0);
        MhaTbl mha1 = mhaTblDao.queryById(mhaId1);
        if (mha0 == null || mha1 == null) {
            logger.error("mha not exist, mhaGroupId: {}, mhaId0: {}, mhaId1: {}", mhaGroupId, mhaId0, mhaId1);
            throw ConsoleExceptionUtils.message("mha not exist");
        }

        ClusterMhaMapTbl clusterMhaMap0 = clusterMhaMapTblDao.queryByMhaIds(Lists.newArrayList(mhaId0), BooleanEnum.FALSE.getCode()).get(0);
        ClusterMhaMapTbl clusterMhaMap1 = clusterMhaMapTblDao.queryByMhaIds(Lists.newArrayList(mhaId1), BooleanEnum.FALSE.getCode()).get(0);
        ClusterTbl clusterTbl0 = clusterTblDao.queryByPk(clusterMhaMap0.getClusterId());
        ClusterTbl clusterTbl1 = clusterTblDao.queryByPk(clusterMhaMap1.getClusterId());

        MhaTblV2 newMha0 = buildMhaTblV2(mha0, mhaGroup, clusterTbl0);
        MhaTblV2 newMha1 = buildMhaTblV2(mha1, mhaGroup, clusterTbl1);
        insertOrUpdateMha(newMha0);
        insertOrUpdateMha(newMha1);
    }

    private void insertOrUpdateMha(MhaTblV2 mhaTbl) throws Exception {
        MhaTblV2 oldMhaTbl = mhaTblV2Dao.queryByPk(mhaTbl.getId());
        if (oldMhaTbl != null) {
            mhaTblV2Dao.update(mhaTbl);
        } else {
            mhaTblV2Dao.insert(new DalHints().enableIdentityInsert(), mhaTbl);
        }
    }

    private MhaTblV2 buildMhaTblV2(MhaTbl mhaTbl, MhaGroupTbl mhaGroupTbl, ClusterTbl clusterTbl) {
        MhaTblV2 newMhaTbl = new MhaTblV2();
        newMhaTbl.setId(mhaTbl.getId());
        newMhaTbl.setMhaName(mhaTbl.getMhaName());
        newMhaTbl.setDcId(mhaTbl.getDcId());
        newMhaTbl.setApplyMode(mhaTbl.getApplyMode());
        newMhaTbl.setMonitorSwitch(mhaTbl.getMonitorSwitch());
        newMhaTbl.setBuId(clusterTbl.getBuId());
        newMhaTbl.setClusterName(clusterTbl.getClusterName());
        newMhaTbl.setAppId(clusterTbl.getClusterAppId());
        newMhaTbl.setDeleted(BooleanEnum.FALSE.getCode());

        newMhaTbl.setReadUser(mhaGroupTbl.getReadUser());
        newMhaTbl.setReadPassword(mhaGroupTbl.getReadPassword());
        newMhaTbl.setWriteUser(mhaGroupTbl.getWriteUser());
        newMhaTbl.setWritePassword(mhaGroupTbl.getWritePassword());
        newMhaTbl.setMonitorUser(mhaGroupTbl.getMonitorUser());
        newMhaTbl.setMonitorPassword(mhaGroupTbl.getMonitorPassword());

        return newMhaTbl;
    }
}
