package com.ctrip.framework.drc.console.service.v2.impl;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorGroupTblDao;
import com.ctrip.framework.drc.console.dao.ReplicatorTblDao;
import com.ctrip.framework.drc.console.dao.ResourceTblDao;
import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorGroupTbl;
import com.ctrip.framework.drc.console.dao.entity.ReplicatorTbl;
import com.ctrip.framework.drc.console.dao.entity.v2.MhaTblV2;
import com.ctrip.framework.drc.console.dao.v2.MhaTblV2Dao;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.DrcAccountTypeEnum;
import com.ctrip.framework.drc.console.exception.ConsoleException;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.param.v2.security.Account;
import com.ctrip.framework.drc.console.service.v2.DbMetaCorrectService;
import com.ctrip.framework.drc.console.service.v2.security.AccountService;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.config.ConsoleConfig.SHOULD_AFFECTED_ROWS;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

/**
 * @ClassName DbMetaCorrectServiceImpl
 * @Author haodongPan
 * @Date 2023/7/28 18:01
 * @Version: $
 */
@Service
public class DbMetaCorrectServiceImpl implements DbMetaCorrectService {
    
    
    @Autowired private MhaTblV2Dao mhaTblV2Dao;
    
    @Autowired private ReplicatorGroupTblDao rGroupTblDao;
    
    @Autowired private ReplicatorTblDao replicatorTblDao;
    
    @Autowired private ResourceTblDao resourceTblDao;
    
    @Autowired private MachineTblDao machineTblDao;

    @Autowired private MonitorTableSourceProvider monitorTableSourceProvider;
    
    @Autowired private AccountService accountService;
    
    private final Logger logger = LoggerFactory.getLogger(getClass());


    @Override
    public void mhaInstancesChange(MhaInstanceGroupDto mhaInstanceGroupDto, MhaTblV2 mhaTblV2) throws Exception {
        String mhaName = mhaInstanceGroupDto.getMhaName();
        logger.info("[[task=syncMhaTask,mha={}]] check mha instances change,master is {}", mhaName,mhaInstanceGroupDto.getMaster());
        List<MachineTbl> machinesInMetaDb = machineTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
        // change targets
        final List<MachineTbl> insertMachines = Lists.newArrayList();
        final List<MachineTbl> updateMachines = Lists.newArrayList();
        final List<MachineTbl> deleteMachines = Lists.newArrayList();

        this.checkChange(mhaInstanceGroupDto,machinesInMetaDb,mhaTblV2,insertMachines,updateMachines,deleteMachines);

        if (monitorTableSourceProvider.getSwitchSyncMhaUpdateAll().equalsIgnoreCase(SWITCH_STATUS_ON)) {
            logger.info("[[task=syncMhaTask,mha={}]] switch turn on,updateAll change to meta db",mhaName);
            if (!CollectionUtils.isEmpty(insertMachines)) {
                this.beforeInsert(insertMachines,mhaTblV2);
                int[] ints = machineTblDao.batchInsert(insertMachines);
                loggingAction(mhaName,ints,"Insert");
            }
            if (!CollectionUtils.isEmpty(updateMachines)) {
                int[] updates = machineTblDao.batchUpdate(updateMachines);
                loggingAction(mhaName,updates,"Update");
            }
            if (!CollectionUtils.isEmpty(deleteMachines)) {
                int[] deletes = machineTblDao.batchLogicalDelete(deleteMachines);
                loggingAction(mhaName,deletes,"Delete");
            }
        } else {
            logger.info("[[task=syncMhaTask,mha={}]] switch turn off,will not updateAll change to meta db",mhaName);
        }

    }

    @Override
    public void mhaInstancesChange(List<MachineTbl> machinesInDba, MhaTblV2 mhaTblV2) {
        try {
            String mhaName = mhaTblV2.getMhaName();
            List<MachineTbl> machinesInMetaDb = machineTblDao.queryByMhaId(mhaTblV2.getId(), BooleanEnum.FALSE.getCode());
            // change targets
            final List<MachineTbl> insertMachines = Lists.newArrayList();
            final List<MachineTbl> updateMachines = Lists.newArrayList();
            final List<MachineTbl> deleteMachines = Lists.newArrayList();
            this.checkChange(machinesInDba, machinesInMetaDb, mhaTblV2, insertMachines, updateMachines, deleteMachines);

            String type = "DRC.syncMhaFromDba";
            if (!CollectionUtils.isEmpty(insertMachines)) {
                this.beforeInsert(insertMachines, mhaTblV2);
                int[] ints = machineTblDao.batchInsert(insertMachines);
                loggingAction(mhaName, ints, "Insert", type);
            }
            if (!CollectionUtils.isEmpty(updateMachines)) {
                int[] updates = machineTblDao.batchUpdate(updateMachines);
                loggingAction(mhaName, updates, "Update", type);
            }
            if (!CollectionUtils.isEmpty(deleteMachines)) {
                int[] deletes = machineTblDao.batchLogicalDelete(deleteMachines);
                loggingAction(mhaName, deletes, "Delete", type);
            }
        } catch (Exception e) {
            throw new ConsoleException("mhaInstancesChange fail: " + e.getMessage(), e);
        }
    }

    @Override
    public ApiResult mhaMasterDbChange(String mhaName, String ip, int port) {
        try {
            MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mhaName);
            if (null == mhaTblV2) {
                logger.error("[[mha={}]]no such mha", mhaName);
                return ApiResult.getInstance(0, ResultCode.HANDLE_FAIL.getCode(), "no such mha " + mhaName);
            }
            List<MachineTbl> machineTblToBeUpdated = checkMachinesInUse(mhaTblV2.getId(), mhaName, ip, port);
            if (machineTblToBeUpdated.size() == 0) {
                return ApiResult.getInstance(0, ResultCode.HANDLE_SUCCESS.getCode(), mhaName + ' ' + ip + ':' + port + " already master");
            }
            int[] affectedUpdateArr = machineTblDao.batchUpdate(machineTblToBeUpdated);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.switch." + mhaName, ip);
            int updateAffected = Arrays.stream(affectedUpdateArr).sum();
            return SHOULD_AFFECTED_ROWS == updateAffected ? 
                    ApiResult.getInstance(updateAffected, ResultCode.HANDLE_SUCCESS.getCode(), "update " + mhaName + " master instance succeeded") :
                    ApiResult.getInstance(updateAffected, ResultCode.HANDLE_FAIL.getCode(), mhaName + ", updated: " + updateAffected);
        } catch (Throwable t) {
            logger.error("Fail update {} master instance", mhaName, t);
            return ApiResult.getInstance(0, ResultCode.HANDLE_FAIL.getCode(), "Fail update master instance as " + t);
        }
    }

    @Override
    public void batchMhaMasterDbChange(List<MhaInstanceGroupDto> mhaInstanceGroupDtos) throws Exception {
        List<String> mhaNames = mhaInstanceGroupDtos.stream().map(MhaInstanceGroupDto::getMhaName).collect(Collectors.toList());
        logger.info("mhaMasterDbChange mhaName: {}", mhaNames);
        List<MhaTblV2> mhaTblV2s = mhaTblV2Dao.queryByMhaNames(mhaNames, BooleanEnum.FALSE.getCode());
        List<Long> mhaIds = mhaTblV2s.stream().map(MhaTblV2::getId).collect(Collectors.toList());

        Map<String, MhaInstanceGroupDto> mhaDtoMap = mhaInstanceGroupDtos.stream().collect(Collectors.toMap(MhaInstanceGroupDto::getMhaName, Function.identity()));
        Map<Long, String> mhaIdToName = mhaTblV2s.stream().collect(Collectors.toMap(MhaTblV2::getId, MhaTblV2::getMhaName));

        List<MachineTbl> machineTbls = machineTblDao.queryByMhaIds(mhaIds);
        Map<Long, List<MachineTbl>> machineMap = machineTbls.stream().collect(Collectors.groupingBy(MachineTbl::getMhaId));

        for (Map.Entry<Long, List<MachineTbl>> entry : machineMap.entrySet()) {
            long mhaId = entry.getKey();
            List<MachineTbl> machines = entry.getValue();

            MhaInstanceGroupDto mhaInstanceGroupDto = mhaDtoMap.get(mhaIdToName.get(mhaId));
            String masterIp = mhaInstanceGroupDto.getMaster().getIp();
            int masterPort = mhaInstanceGroupDto.getMaster().getPort();
            for (MachineTbl machineTbl : machines) {
                if (machineTbl.getIp().equalsIgnoreCase(masterIp) && machineTbl.getPort() == masterPort) {
                    machineTbl.setMaster(BooleanEnum.TRUE.getCode());
                } else {
                    machineTbl.setMaster(BooleanEnum.FALSE.getCode());
                }
            }
        }

        machineTblDao.update(machineTbls);
    }

    @VisibleForTesting
    protected List<MachineTbl> checkMachinesInUse(Long mhaId, String mhaName, String ip, int port) throws SQLException {
        List<MachineTbl> machineTblToBeUpdated = Lists.newArrayList();
        List<MachineTbl> machineTbls = machineTblDao.queryByMhaId(mhaId,BooleanEnum.FALSE.getCode());
        for (MachineTbl machineTbl : machineTbls) {
            if (ip.equalsIgnoreCase(machineTbl.getIp()) && port == machineTbl.getPort()) {
                if (machineTbl.getMaster().equals(BooleanEnum.FALSE.getCode())) {
                    machineTbl.setMaster(BooleanEnum.TRUE.getCode());
                    machineTblToBeUpdated.add(machineTbl);
                    logger.info("[[mha={}]]todo slave->master: {} ", mhaName, machineTbl.getIp() + ":" + machineTbl.getPort());
                }
            } else if (machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode())) {
                machineTbl.setMaster(BooleanEnum.FALSE.getCode());
                machineTblToBeUpdated.add(machineTbl);
                logger.info("[[mha={}]]todo master->slave: {} ", mhaName,  machineTbl.getIp() + ":" + machineTbl.getPort());
            }
        }
        return machineTblToBeUpdated;
    }
    
    private void loggingAction(String mha,int[] effects,String action) {
        int affectRows = Arrays.stream(effects).sum();
        logger.info("[[task=syncMhaTask,mha={},action={}]] affectRows:{}",mha,action,affectRows);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.syncMhaFromDal." + mha,action,affectRows);
    }

    private void loggingAction(String mha, int[] effects, String action, String type) {
        int affectRows = Arrays.stream(effects).sum();
        DefaultEventMonitorHolder.getInstance().logEvent(String.join(type, mha), action, affectRows);
    }


    private void beforeInsert(List<MachineTbl> insertMachines,MhaTblV2 mhaTblV2) throws Exception{
        String mhaName = mhaTblV2.getMhaName();
        for(MachineTbl machine : insertMachines) {
            String ip = machine.getIp();
            Integer port = machine.getPort();
            Integer master = machine.getMaster();
            Account account = accountService.getAccount(mhaTblV2, DrcAccountTypeEnum.DRC_CONSOLE);
            String uuid = MySqlUtils.getUuid(ip, port, account.getUser(), account.getPassword(),
                    BooleanEnum.TRUE.getCode().equals(master));
            if (null == uuid) {
                logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaName, ip, port);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.insert.fail." + mhaName, ip);
                throw new Exception(mhaName + " cannot get uuid " + ip + ":" + port);
            }
            machine.setUuid(uuid);
        }
    }

    @VisibleForTesting
    protected void checkChange(final MhaInstanceGroupDto mhaInstancesInDal, final List<MachineTbl> machinesInMetaDb,
            final MhaTblV2 mhaTblV2, final List<MachineTbl> insertMachines,
            final List<MachineTbl> updateMachines, final List<MachineTbl> deleteMachines) {
        List<MachineTbl> machinesInDal = mhaInstancesInDal.transferToMachine();
        this.getInsertMachines(machinesInDal,machinesInMetaDb,insertMachines,mhaTblV2);
        this.getDeleteMachines(machinesInDal,machinesInMetaDb,deleteMachines,mhaTblV2);
        this.getUpdateMachines(machinesInDal,machinesInMetaDb,updateMachines,mhaTblV2);

    }

    @VisibleForTesting
    protected void checkChange(final List<MachineTbl> machinesInDba, final List<MachineTbl> machinesInMetaDb,
                               final MhaTblV2 mhaTblV2, final List<MachineTbl> insertMachines,
                               final List<MachineTbl> updateMachines, final List<MachineTbl> deleteMachines) {
        this.getInsertMachines(machinesInDba,machinesInMetaDb,insertMachines,mhaTblV2);
        this.getDeleteMachines(machinesInDba,machinesInMetaDb,deleteMachines,mhaTblV2);
        this.getUpdateMachines(machinesInDba,machinesInMetaDb,updateMachines,mhaTblV2);

    }

    private void getInsertMachines(List<MachineTbl> machinesInDal,
            List<MachineTbl> machinesInMetaDb,
            List<MachineTbl> insertMachines, MhaTblV2 mhaTblV2) {

        machinesInDal.stream().filter(
                remote -> machinesInMetaDb.stream().noneMatch(
                        local -> remote.getIp().equalsIgnoreCase(local.getIp())
                                && remote.getPort().equals(local.getPort())
                )
        ).forEach(
                add -> {
                    MachineTbl toBeAdded = new MachineTbl(add.getIp(), add.getPort(), add.getMaster());
                    toBeAdded.setMhaId(mhaTblV2.getId());
                    logger.info("[[task=syncMhaTask,mha={},action=Insert]] mysql machine to be inserted :{}",
                            mhaTblV2.getMhaName(),toBeAdded );
                    insertMachines.add(toBeAdded);
                }
        );
    }

    private void getDeleteMachines(List<MachineTbl> machinesInDal,
            List<MachineTbl> machinesInMetaDb,
            List<MachineTbl> deleteMachines, MhaTblV2 mhaTblV2) {
        machinesInMetaDb.stream().filter(
                local -> machinesInDal.stream().noneMatch(
                        remote -> local.getIp().equalsIgnoreCase(remote.getIp())
                                && local.getPort().equals(remote.getPort())
                )
        ).forEach(
                delete -> {
                    MachineTbl toBeDeleted = new MachineTbl(delete.getIp(), delete.getPort(), delete.getMaster());
                    toBeDeleted.setDeleted(BooleanEnum.TRUE.getCode());
                    toBeDeleted.setMhaId(mhaTblV2.getId());
                    toBeDeleted.setId(delete.getId());
                    logger.info("[[task=syncMhaTask,mha={},action=Delete]] mysql machine to be logicalDelete :{}",
                            mhaTblV2.getMhaName(), toBeDeleted);
                    deleteMachines.add(toBeDeleted);
                }
        );
    }

    private void getUpdateMachines(List<MachineTbl> machinesInDal,
            List<MachineTbl> machinesInMetaDb,
            List<MachineTbl> updateMachines,MhaTblV2 mhaTblV2) {
        machinesInDal.forEach(
                remote -> machinesInMetaDb.stream().filter(
                        local -> remote.getIp().equalsIgnoreCase(local.getIp())
                                && remote.getPort().equals(local.getPort())
                ).findFirst().ifPresent(
                        update -> {
                            if (!remote.getMaster().equals(update.getMaster())) {
                                MachineTbl toBeUpdated = new MachineTbl(update.getIp(), update.getPort(), update.getMaster());
                                toBeUpdated.setMaster(remote.getMaster());
                                toBeUpdated.setMhaId(mhaTblV2.getId());
                                toBeUpdated.setId(update.getId());
                                logger.info("[[task=syncMhaTask,mha={},action=Update]] mysql machine master status change:{}",
                                        mhaTblV2.getMhaName(),toBeUpdated);
                                updateMachines.add(toBeUpdated);
                            }
                        }
                )
        );
    }

    private Map<String, ReplicatorTbl> getIpReplicatorMap(String mha) throws SQLException {
        Map<String, ReplicatorTbl> ip2Replicator = Maps.newHashMap();
        MhaTblV2 mhaTblV2 = mhaTblV2Dao.queryByMhaName(mha);
        ReplicatorGroupTbl replicatorGroupTbl = rGroupTblDao.queryByMhaId(mhaTblV2.getId());
        if(null != replicatorGroupTbl) {
            try {
                List<ReplicatorTbl> replicatorTbls = replicatorTblDao.
                        queryByRGroupIds(Lists.newArrayList(replicatorGroupTbl.getId()),BooleanEnum.FALSE.getCode());
                for(ReplicatorTbl replicatorTbl : replicatorTbls) {
                    Long resourceId = replicatorTbl.getResourceId();
                    String ip = resourceTblDao.queryByPk(resourceId).getIp();
                    ip2Replicator.put(ip, replicatorTbl);
                }
            } catch (SQLException e) {
                logger.error("Fail getIpReplicatorMap for {}, ", mha, e);
            }
        }
        return ip2Replicator;
    }
}
