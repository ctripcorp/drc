package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.*;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.ReadableErrorDefEnum;
import com.ctrip.framework.drc.console.param.v2.MhaQuery;
import com.ctrip.framework.drc.console.pojo.domain.DcDo;
import com.ctrip.framework.drc.console.service.v2.MetaInfoServiceV2;
import com.ctrip.framework.drc.console.service.v2.MhaServiceV2;
import com.ctrip.framework.drc.console.utils.ConsoleExceptionUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.platform.dal.dao.DalHints;
import com.ctrip.platform.dal.dao.KeyHolder;
import com.ctrip.platform.dal.dao.annotation.DalTransactional;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
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
    private MachineTblDao machineTblDao;

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
    public String getMysqlUuid(String mhaName, String ip, int port, boolean master) throws Exception {
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (mhaTblV2 == null) {
            throw ConsoleExceptionUtils.message(String.format("mha: %s not exist", mhaName));
        }
        String uuid = MySqlUtils.getUuid(ip, port, mhaTblV2.getReadUser(), mhaTblV2.getReadPassword(), master);
        return uuid;
    }

    @Override
    public boolean recordMhaInstances(MhaInstanceGroupDto dto) throws Exception {
        String mhaName = dto.getMhaName();
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName, BooleanEnum.FALSE.getCode());
        if (null == mhaTblV2) {
            logger.info("mha({}) null", mhaName);
            return false;
        }
        Long mhaId = mhaTblV2.getId();
        List<MachineTbl> currentMachineTbls = machineTblDao.queryByMhaId(mhaId, BooleanEnum.FALSE.getCode());
        if (dto.getMaster() != null) {
            logger.info("add master machine-{} in mha-{}", dto.getMaster(), dto.getMhaName());
            return checkMasterMachineMatch(dto, currentMachineTbls, mhaTblV2);
        } else if (dto.getSlaves() != null) {
            logger.info("add slave machine-{} in mha-{}", dto.getMaster(), dto.getMhaName());
            checkSlaveMachines(dto, currentMachineTbls, mhaTblV2);
            return true;
        }
        return false;
    }

    private boolean checkMasterMachineMatch(MhaInstanceGroupDto dto, List<MachineTbl> currentMachineTbls, MhaTblV2 mhaTblV2) throws Exception {
        MhaInstanceGroupDto.MySQLInstance master = dto.getMaster();
        if (currentMachineTbls.size() == 0) {
            insertMasterMachine(mhaTblV2, master.getIp(), master.getPort(), master.getUuid());
            return true;
        } else {
            // check: alert if master not exist in current or current master does not match
            boolean masterMatch = false;
            for (MachineTbl machineTbl : currentMachineTbls) {
                if (BooleanEnum.TRUE.getCode().equals(machineTbl.getMaster()) && machineTbl.getIp().equalsIgnoreCase(master.getIp()) && machineTbl.getPort().equals(master.getPort())) {
                    masterMatch = true;
                    break;
                }
            }
            if (!masterMatch) {
                logger.error("new master {} {}:{} after init", mhaTblV2.getMhaName(), master.getIp(), master.getPort());
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.instance.unnotified", mhaTblV2.getMhaName());
            }
            return masterMatch;
        }
    }

    private void checkSlaveMachines(MhaInstanceGroupDto dto, List<MachineTbl> currentMachineTbls, MhaTblV2 mhaTblV2) throws Exception {
        List<String> currentMachineIps = currentMachineTbls.stream().map(MachineTbl::getIp).collect(Collectors.toList());
        List<MhaInstanceGroupDto.MySQLInstance> slaves = dto.getSlaves();
        for (MhaInstanceGroupDto.MySQLInstance slave : slaves) {
            String ip = slave.getIp();
            if (!currentMachineIps.contains(ip)) {
                int port = slave.getPort();
                insertSlaveMachine(mhaTblV2, ip, port, slave.getUuid());
            }
        }
    }

    private int insertMasterMachine(MhaTblV2 mhaTblV2, String ip, int port, String suppliedUuid) throws Exception {
        String mhaName = mhaTblV2.getMhaName();
        long mhaId = mhaTblV2.getId();
        logger.info("[[mha={}]]no such master {}:{}, try insert", mhaTblV2.getMhaName(), ip, port);
        String uuid = StringUtils.isNotBlank(suppliedUuid) ? suppliedUuid : MySqlUtils.getUuid(ip, port, mhaTblV2.getMonitorUser(), mhaTblV2.getMonitorPassword(), true);
        if (null == uuid) {
            logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaTblV2.getMhaName(), ip, port);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.insert.fail." + mhaName, ip);
            throw ConsoleExceptionUtils.message(mhaName + " cannot get uuid " + ip);
        }
        logger.info("[[mha={}]] insert master {}:{}", mhaName, ip, port);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.insert." + mhaName, ip);
        return insertMachine(ip, port, uuid, BooleanEnum.TRUE.getCode(), mhaId);
    }

    private int insertSlaveMachine(MhaTblV2 mhaTblV2, String ip, int port, String suppliedUuid) throws Exception {
        String mhaName = mhaTblV2.getMhaName();
        long mhaId = mhaTblV2.getId();
        logger.info("[[mha={}]]no such slave {}:{}, try insert", mhaName, ip, port);
        String uuid = StringUtils.isNotBlank(suppliedUuid) ? suppliedUuid : MySqlUtils.getUuid(ip, port, mhaTblV2.getMonitorUser(), mhaTblV2.getMonitorPassword(), false);
        if (null == uuid) {
            logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaName, ip, port);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.slave.insert.fail." + mhaName, ip);
            throw ConsoleExceptionUtils.message(mhaName + " cannot get uuid " + ip);
        }
        logger.info("[[mha={}]] insert slave {}:{}", mhaName, ip, port);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.slave.insert." + mhaName, ip);
        return insertMachine(ip, port, uuid, BooleanEnum.FALSE.getCode(), mhaId);
    }

    private int insertMachine(String ip, int port, String uuid, int master, long mhaId) throws Exception {
        MachineTbl daoPojo = new MachineTbl();
        daoPojo.setIp(ip);
        daoPojo.setPort(port);
        daoPojo.setMaster(master);
        daoPojo.setUuid(uuid);
        daoPojo.setMhaId(mhaId);

        KeyHolder keyHolder = new KeyHolder();
        return machineTblDao.insert(new DalHints(), keyHolder, daoPojo);
    }
}
