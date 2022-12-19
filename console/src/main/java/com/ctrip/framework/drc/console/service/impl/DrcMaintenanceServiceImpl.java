package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.MachineTblDao;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.DrcMaintenanceService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.platform.dal.dao.DalPojo;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
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
import java.util.stream.Collectors;

import static com.ctrip.framework.drc.console.config.ConsoleConfig.SHOULD_AFFECTED_ROWS;
import static com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider.SWITCH_STATUS_ON;

@Service
public class DrcMaintenanceServiceImpl implements DrcMaintenanceService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private DalUtils dalUtils = DalUtils.getInstance();

    private MachineTblDao machineTblDao = dalUtils.getMachineTblDao();
    
    @Autowired
    private MetaInfoServiceImpl metaInfoService;

    @Autowired
    private DefaultCurrentMetaManager currentMetaManager;

    @Autowired
    private MonitorTableSourceProvider monitorTableSourceProvider;

    public ApiResult changeMasterDb(String mhaName, String ip, int port) {
        try {
            Long mhaId = dalUtils.getId(TableEnum.MHA_TABLE, mhaName);
            if (null == mhaId) {
                logger.error("[[mha={}]]no such mha", mhaName);
                return ApiResult.getInstance(0, ResultCode.HANDLE_FAIL.getCode(), "no such mha " + mhaName);
            }
            List<String> masterDb = Lists.newArrayList();
            List<String> slaveDb = Lists.newArrayList();
            List<MachineTbl> machineTblToBeUpdated = checkMachinesInUse(masterDb, slaveDb, mhaId, mhaName, ip, port);
            if (machineTblToBeUpdated.size() == 0) {
                return ApiResult.getInstance(0, ResultCode.HANDLE_SUCCESS.getCode(), mhaName + ' ' + ip + ':' + port + " already master");
            }
            int insertAffected = 0 == masterDb.size() ? insertMasterMachine(mhaId, mhaName, ip, port, null) : 0;
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.switch." + mhaName, ip);
            int[] AffectedUpdateArr = dalUtils.getMachineTblDao().batchUpdate(machineTblToBeUpdated);
            int updateAffected = Arrays.stream(AffectedUpdateArr).reduce(0, Integer::sum);
            int allAffected = insertAffected + updateAffected;
            return SHOULD_AFFECTED_ROWS == allAffected ? ApiResult.getInstance(allAffected, ResultCode.HANDLE_SUCCESS.getCode(), "update " + mhaName + " master instance succeeded, u" + updateAffected + 'i' + insertAffected) 
                    : ApiResult.getInstance(allAffected, ResultCode.HANDLE_FAIL.getCode(), mhaName + " inserted: " + insertAffected + ", updated: " + updateAffected);
        } catch (Throwable t) {
            logger.error("Fail update {} master instance", mhaName, t);
            return ApiResult.getInstance(0, ResultCode.HANDLE_FAIL.getCode(), "Fail update master instance as " + t);
        }
    }

    private List<MachineTbl> checkMachinesInUse(List<String> masterDb, List<String> slaveDb, Long mhaId, String mhaName, String ip, int port) throws SQLException {
        List<MachineTbl> machineTblToBeUpdated = Lists.newArrayList();
        List<MachineTbl> machineTbls = dalUtils.getMachineTblDao().queryAll().stream().filter(m -> (m.getDeleted().equals(BooleanEnum.FALSE.getCode()) && mhaId.equals(m.getMhaId()))).collect(Collectors.toList());
        for (MachineTbl machineTbl : machineTbls) {
            if (ip.equalsIgnoreCase(machineTbl.getIp()) && port == machineTbl.getPort()) {
                if (machineTbl.getMaster().equals(BooleanEnum.FALSE.getCode())) {
                    masterDb.add(ip + ':' + port);
                    machineTbl.setMaster(BooleanEnum.TRUE.getCode());
                    machineTblToBeUpdated.add(machineTbl);
                }
            } else if (machineTbl.getMaster().equals(BooleanEnum.TRUE.getCode())) {
                slaveDb.add(machineTbl.getIp() + ':' + machineTbl.getPort());
                machineTbl.setMaster(BooleanEnum.FALSE.getCode());
                machineTblToBeUpdated.add(machineTbl);
            }
        }
        logger.info("[[mha={}]]slave->master: {}, master->slave: {} ", mhaName, masterDb, slaveDb);
        return machineTblToBeUpdated;
    }

    public Boolean updateMhaInstances(MhaInstanceGroupDto dto) throws Throwable {
        String mhaName = dto.getMhaName();
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaName.equals(p.getMhaName())).findFirst().orElse(null);
        if (null == mhaTbl) {
            logger.info("mha({}) null", mhaName);
            return false;
        }
        Long mhaId = mhaTbl.getId();
        List<MachineTbl> currentMachineTbls = dalUtils.getMachineTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaId.equals(p.getMhaId())).collect(Collectors.toList());
        if (checkMasterMachineMatch(dto, currentMachineTbls, mhaId, mhaName)) {
            checkSlaveMachines(dto, currentMachineTbls, mhaId);
            return true;
        }
        return false;
    }
    
    public Boolean recordMhaInstances(MhaInstanceGroupDto dto) throws Throwable {
        String mhaName = dto.getMhaName();
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaName.equals(p.getMhaName())).findFirst().orElse(null);
        if (null == mhaTbl) {
            logger.info("mha({}) null", mhaName);
            return false;
        }
        Long mhaId = mhaTbl.getId();
        List<MachineTbl> currentMachineTbls = dalUtils.getMachineTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaId.equals(p.getMhaId())).collect(Collectors.toList());
        if (dto.getMaster() != null) {
            logger.info("add master machine-{} in mha-{}",dto.getMaster(),dto.getMhaName());
            return checkMasterMachineMatch(dto, currentMachineTbls, mhaId, mhaName);
        } else if (dto.getSlaves() != null) {
            logger.info("add slave machine-{} in mha-{}",dto.getMaster(),dto.getMhaName());
            checkSlaveMachines(dto, currentMachineTbls, mhaId);
            return true;
        }
        return false;
    }

    private boolean checkMasterMachineMatch(MhaInstanceGroupDto dto, List<MachineTbl> currentMachineTbls, Long mhaId, String mhaName) throws Throwable {
        MhaInstanceGroupDto.MySQLInstance master = dto.getMaster();
        if (currentMachineTbls.size() == 0) {
            insertMasterMachine(mhaId, mhaName, master.getIp(), master.getPort(), master.getUuid());
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
                logger.error("new master {} {}:{} after init", mhaName, master.getIp(), master.getPort());
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.instance.unnotified", mhaName);
            }
            return masterMatch;
        }
    }

    private void checkSlaveMachines(MhaInstanceGroupDto dto, List<MachineTbl> currentMachineTbls, Long mhaId) throws Throwable {
        List<String> currentMachineIps = currentMachineTbls.stream().map(MachineTbl::getIp).collect(Collectors.toList());
        List<MhaInstanceGroupDto.MySQLInstance> slaves = dto.getSlaves();
        for (MhaInstanceGroupDto.MySQLInstance slave : slaves) {
            String ip = slave.getIp();
            if (!currentMachineIps.contains(ip)) {
                String mhaName = dto.getMhaName();
                int port = slave.getPort();
                insertSlaveMachine(mhaId, mhaName, ip, port, slave.getUuid());
            }
        }
        
    }

    private int insertMasterMachine(Long mhaId, String mhaName, String ip, int port, String suppliedUuid) throws Throwable {
        logger.info("[[mha={}]]no such master {}:{}, try insert", mhaName, ip, port);
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryByPk(mhaId);
        MhaGroupTbl mhaGroupTbl = metaInfoService.getMhaGroupForMha(mhaTbl.getMhaName());
        String uuid = StringUtils.isNotBlank(suppliedUuid) ? suppliedUuid : MySqlUtils.getUuid(ip, port, mhaGroupTbl.getMonitorUser(), mhaGroupTbl.getMonitorPassword(), true);
        if (null == uuid) {
            logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaName, ip, port);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.insert.fail." + mhaName, ip);
            throw new Exception(mhaName + " cannot get uuid " + ip);
        }
        logger.info("[[mha={}]] insert master {}:{}", mhaName, ip, port);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.master.insert." + mhaName, ip);
        return dalUtils.insertMachine(ip, port, uuid, BooleanEnum.TRUE, mhaId);
    }

    private int insertSlaveMachine(Long mhaId, String mhaName, String ip, int port, String suppliedUuid) throws Throwable {
        logger.info("[[mha={}]]no such slave {}:{}, try insert", mhaName, ip, port);
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryByPk(mhaId);
        MhaGroupTbl mhaGroupTbl = metaInfoService.getMhaGroupForMha(mhaTbl.getMhaName());
        String uuid = StringUtils.isNotBlank(suppliedUuid) ? suppliedUuid : MySqlUtils.getUuid(ip, port, mhaGroupTbl.getMonitorUser(), mhaGroupTbl.getMonitorPassword(), false);
        if (null == uuid) {
            logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaName, ip, port);
            DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.slave.insert.fail." + mhaName, ip);
            throw new Exception(mhaName + " cannot get uuid " + ip);
        }
        logger.info("[[mha={}]] insert slave {}:{}", mhaName, ip, port);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.slave.insert." + mhaName, ip);
        return dalUtils.insertMachine(ip, port, uuid, BooleanEnum.FALSE, mhaId);
    }

    public void updateMhaGroup(String srcMha, String destMha, EstablishStatusEnum establishStatusEnum) throws SQLException {
        Long mhaGroupId = metaInfoService.getMhaGroupId(srcMha, destMha);
        MhaGroupTbl mhaGroupTbl = dalUtils.getMhaGroupTblDao().queryByPk(mhaGroupId);
        if (null != mhaGroupTbl) {
            mhaGroupTbl.setDrcEstablishStatus(establishStatusEnum.getCode());
            dalUtils.getMhaGroupTblDao().update(mhaGroupTbl);
        }
    }

    public Boolean changeMhaGroupStatus(MhaGroupPair mhaGroupPair, Integer status) throws Exception {
        MhaGroupTbl mhaGroupTbl = metaInfoService.getMhaGroup(mhaGroupPair.getSrcMha(), mhaGroupPair.getDestMha());
        mhaGroupTbl.setDrcEstablishStatus(status);
        dalUtils.getMhaGroupTblDao().update(mhaGroupTbl);
        return true;
    }

    public void updateMhaDnsStatus(String mha, BooleanEnum booleanEnum) throws Throwable {
        List<DalPojo> pojos = TableEnum.MHA_TABLE.getAllPojos();
        MhaTbl mhaTbl = (MhaTbl) pojos.stream().filter(p -> (mha.equalsIgnoreCase(((MhaTbl) p).getMhaName()))).findFirst().orElse(null);

        if (null == mhaTbl) {
            logger.info("mha({}) null", mha);
            return;
        }

        mhaTbl.setDnsStatus(booleanEnum.getCode());
        dalUtils.getMhaTblDao().update(mhaTbl);
    }

    public boolean inputResource(String ip, String dc, String description) throws Exception {
        Long dcId = dalUtils.getId(TableEnum.DC_TABLE, dc);
        ModuleEnum moduleEnum = ModuleEnum.getModuleEnum(description);
        dalUtils.updateOrCreateResource(ip, dcId, moduleEnum);
        return true;
    }

    public int deleteResource(String ip) throws Exception {
        ResourceTbl resourceTbl = dalUtils.getResourceTblDao().queryAll().stream().filter(p -> p.getIp().equalsIgnoreCase(ip)).findFirst().orElse(null);
        if (null != resourceTbl && BooleanEnum.FALSE.getCode().equals(resourceTbl.getDeleted())) {
            logger.info("[delete] resource : {}", ip);
            resourceTbl.setDeleted(BooleanEnum.TRUE.getCode());
            return dalUtils.getResourceTblDao().update(resourceTbl);
        } else {
            logger.error("[delete] no such resource: {}", ip);
            return 0;
        }
    }

    public int deleteMachine(String ip) throws Exception {
        MachineTbl machineTbl = dalUtils.getMachineTblDao().queryAll().stream().filter(p -> p.getIp().equalsIgnoreCase(ip) && BooleanEnum.FALSE.getCode().equals(p.getDeleted())).findFirst().orElse(null);
        if (null != machineTbl) {
            if (BooleanEnum.TRUE.getCode().equals(machineTbl.getMaster())) {
                logger.warn("[delete] machine({}) is master, delete not allowed", ip);
                return 0;
            } else {
                logger.info("[delete] machine: {}", ip);
                machineTbl.setDeleted(BooleanEnum.TRUE.getCode());
                return dalUtils.getMachineTblDao().update(machineTbl);
            }
        } else {
            logger.warn("[delete] no such machine: {}", ip);
            return 0;
        }
    }

    public ApiResult deleteRoute(String routeOrgName, String srcDcName, String dstDcName, String tag) {
        try {
            Long buId = dalUtils.getId(TableEnum.BU_TABLE, routeOrgName);
            Long srcDcId = dalUtils.getId(TableEnum.DC_TABLE, srcDcName);
            Long dstDcId = dalUtils.getId(TableEnum.DC_TABLE, dstDcName);
            RouteTbl routeTbl = dalUtils.getRouteTblDao().queryAll().stream()
                    .filter(p -> p.getDeleted().equals(BooleanEnum.FALSE.getCode()) &&
                            p.getRouteOrgId().equals(buId) &&
                            p.getSrcDcId().equals(srcDcId) &&
                            p.getDstDcId().equals(dstDcId) &&
                            p.getTag().equalsIgnoreCase(tag))
                    .findFirst().orElse(null);
            if (null != routeTbl) {
                routeTbl.setDeleted(BooleanEnum.TRUE.getCode());
                dalUtils.getRouteTblDao().update(routeTbl);
                return ApiResult.getSuccessInstance(String.format("Successfully delete route %s-%s, %s->%s", routeOrgName, tag, srcDcName, dstDcName));
            }
        } catch (SQLException e) {
            logger.error("Failed delete route {}-{}, {}->{}", routeOrgName, tag, srcDcName, dstDcName, e);
        }
        return ApiResult.getFailInstance(String.format("Failed delete route %s-%s, %s->%s", routeOrgName, tag, srcDcName, dstDcName));
    }

    public boolean updateMasterReplicatorIfChange(String mhaName, String newIp) {
        Map<String, ReplicatorTbl> replicators = metaInfoService.getReplicators(mhaName);
        ReplicatorTbl replicatorTbl = replicators.get(newIp);
        if (null == replicatorTbl) {
            logger.error("unknown replicator master ip,mha:{},ip:{}",mhaName,newIp);
        } else {
            if (replicatorTbl.getMaster().equals(BooleanEnum.FALSE.getCode())) {
                List<ReplicatorTbl> replicatorTblsToBeUpdated = Lists.newArrayList();
                
                replicatorTbl.setMaster(BooleanEnum.TRUE.getCode());
                replicatorTblsToBeUpdated.add(replicatorTbl);
                
                ReplicatorTbl oldMaster = replicators.values().stream()
                        .filter(p -> p.getMaster().equals(BooleanEnum.TRUE.getCode())).findFirst().orElse(null);
                if (null != oldMaster) {
                    oldMaster.setMaster(BooleanEnum.FALSE.getCode());
                    replicatorTblsToBeUpdated.add(oldMaster);
                }
                
                try {
                    dalUtils.getReplicatorTblDao().batchUpdate(replicatorTblsToBeUpdated);
                    if (null != oldMaster) {
                        DefaultEventMonitorHolder.getInstance()
                                .logEvent("DRC.replicator.master", String.format("mha:%s,%s->%s",mhaName,oldMaster, newIp));
                    } else {
                        DefaultEventMonitorHolder.getInstance()
                                .logEvent("DRC.replicator.master", String.format("mha:%s,%s",mhaName, newIp));
                    }
                    return true;
                } catch (SQLException e) {
                    logger.error("Fail update master replicator({}), ", newIp, e);
                }
            } else {
                logger.debug("replicator master ip not change,mha:{},ip:{}",mhaName,newIp);
            }
        }
        return false;
    }

    public ApiResult inputDc(String dc) {
        try {
            dalUtils.updateOrCreateDc(dc);
            return ApiResult.getSuccessInstance(String.format("input dc %s succeeded", dc));
        } catch (SQLException e) {
            return ApiResult.getFailInstance(String.format("input dc %s failed: %s", dc, e));
        }
    }

    public ApiResult inputBu(String bu) {
        try {
            dalUtils.updateOrCreateBu(bu);
            return ApiResult.getSuccessInstance(String.format("input bu %s succeeded", bu));
        } catch (SQLException e) {
            return ApiResult.getFailInstance(String.format("input bu %s failed: %s", bu, e));
        }
    }

    public ApiResult inputProxy(ProxyDto proxyDto) {
        try {
            dalUtils.updateOrCreateProxy(proxyDto.toProxyTbl());
            return ApiResult.getSuccessInstance(String.format("input Proxy %s succeeded", proxyDto));
        } catch (Exception e) {
            return ApiResult.getFailInstance(String.format("input Proxy %s failed: %s", proxyDto, e));
        }
    }

    public ApiResult deleteProxy(ProxyDto proxyDto) {
        try {
            ProxyTbl proxyTbl = proxyDto.toProxyTbl();
            proxyTbl.setDeleted(BooleanEnum.TRUE.getCode());
            dalUtils.updateOrCreateProxy(proxyDto.toProxyTbl());
            return ApiResult.getSuccessInstance(String.format("delete Proxy %s succeeded", proxyDto));
        } catch (Exception e) {
            return ApiResult.getFailInstance(String.format("delete Proxy %s failed: %s", proxyDto, e));
        }
    }

    public long inputGroupMapping(Long mhaGroupId, Long mhaId) throws SQLException {
        return dalUtils.updateOrCreateGroupMapping(mhaGroupId, mhaId);
    }

    public int deleteGroupMapping(Long mhaGroupId, Long mhaId) throws SQLException {
        GroupMappingTbl groupMappingTbl = dalUtils.getGroupMappingTblDao().queryAll().stream().filter(p -> (mhaGroupId.equals(p.getMhaGroupId()) && mhaId.equals(p.getMhaId()))).findFirst().orElse(null);
        groupMappingTbl.setDeleted(BooleanEnum.TRUE.getCode());
        return dalUtils.getGroupMappingTblDao().update(groupMappingTbl);
    }

    @Override
    public void mhaInstancesChange(MhaInstanceGroupDto mhaInstanceGroupDto, MhaTbl mhaTbl) throws Exception {
        String mhaName = mhaInstanceGroupDto.getMhaName();
        logger.info("[[task=syncMhaTask,mha={}]] check mha instances change,master is {}", mhaName,mhaInstanceGroupDto.getMaster());
        List<MachineTbl> machinesInMetaDb = machineTblDao.queryByMhaId(mhaTbl.getId(), BooleanEnum.FALSE.getCode());
        // change targets
        final List<MachineTbl> insertMachines = Lists.newArrayList();
        final List<MachineTbl> updateMachines = Lists.newArrayList();
        final List<MachineTbl> deleteMachines = Lists.newArrayList();

        this.checkChange(mhaInstanceGroupDto,machinesInMetaDb,mhaTbl,insertMachines,updateMachines,deleteMachines);
        
        if (monitorTableSourceProvider.getSwitchSyncMhaUpdateAll().equalsIgnoreCase(SWITCH_STATUS_ON)) {
            logger.info("[[task=syncMhaTask,mha={}]] switch turn on,updateAll change to meta db",mhaName);
            if (!CollectionUtils.isEmpty(insertMachines)) {
                this.beforeInsert(insertMachines,mhaTbl);
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
    
    private void loggingAction(String mha,int[] effects,String action) {
        int affectRows = Arrays.stream(effects).sum();
        logger.info("[[task=syncMhaTask,mha={},action={}]] affectRows:{}",mha,action,affectRows);
        DefaultEventMonitorHolder.getInstance().logEvent("DRC.syncMhaFromDal." + mha,action,affectRows);
    }
    
    private void beforeInsert(List<MachineTbl> insertMachines,MhaTbl mhaTbl) throws Exception{
        String mhaName = mhaTbl.getMhaName();
        MhaGroupTbl mhaGroupTbl = metaInfoService.getMhaGroupForMha(mhaTbl.getMhaName());
        for(MachineTbl machine : insertMachines) {
            String ip = machine.getIp();
            Integer port = machine.getPort();
            Integer master = machine.getMaster();
            String uuid = MySqlUtils.getUuid(ip, port, mhaGroupTbl.getMonitorUser(), mhaGroupTbl.getMonitorPassword(), BooleanEnum.TRUE.getCode().equals(master));
            if (null == uuid) {
                logger.error("[[mha={}]]cannot get uuid for {}:{}, do nothing", mhaName, ip, port);
                DefaultEventMonitorHolder.getInstance().logEvent("DRC.mysql.insert.fail." + mhaName, ip);
                throw new Exception(mhaName + " cannot get uuid " + ip + ":" + port);
            }
            machine.setUuid(uuid);
        }
    }

    @VisibleForTesting
    protected void checkChange(final MhaInstanceGroupDto mhaInstancesInDal, final List<MachineTbl> machinesInMetaDb, final MhaTbl mhaTbl,
                             final List<MachineTbl> insertMachines, final List<MachineTbl> updateMachines,
                             final List<MachineTbl> deleteMachines) {
        List<MachineTbl> machinesInDal = mhaInstancesInDal.transferToMachine();
        this.getInsertMachines(machinesInDal,machinesInMetaDb,insertMachines,mhaTbl);
        this.getDeleteMachines(machinesInDal,machinesInMetaDb,deleteMachines,mhaTbl);
        this.getUpdateMachines(machinesInDal,machinesInMetaDb,updateMachines,mhaTbl);

    }

    private void getInsertMachines(List<MachineTbl> machinesInDal,
                                   List<MachineTbl> machinesInMetaDb,
                                   List<MachineTbl> insertMachines,
                                   MhaTbl mha) {
        machinesInDal.stream().filter(
                remote -> machinesInMetaDb.stream().noneMatch(
                        local -> remote.getIp().equalsIgnoreCase(local.getIp())
                                && remote.getPort().equals(local.getPort())
                )
        ).forEach(
                add -> {
                    MachineTbl toBeAdded = new MachineTbl(add.getIp(), add.getPort(), add.getMaster());
                    toBeAdded.setMhaId(mha.getId());
                    logger.info("[[task=syncMhaTask,mha={},action=Insert]] mysql machine to be inserted :{}",
                            mha.getMhaName(),toBeAdded );
                    insertMachines.add(toBeAdded);
                }
        );
    }

    private void getDeleteMachines(List<MachineTbl> machinesInDal,
                                   List<MachineTbl> machinesInMetaDb,
                                   List<MachineTbl> deleteMachines,
                                   MhaTbl mha) {
        machinesInMetaDb.stream().filter(
                local -> machinesInDal.stream().noneMatch(
                        remote -> local.getIp().equalsIgnoreCase(remote.getIp())
                                && local.getPort().equals(remote.getPort())
                )
        ).forEach(
                delete -> {
                    MachineTbl toBeDeleted = new MachineTbl(delete.getIp(), delete.getPort(), delete.getMaster());
                    toBeDeleted.setDeleted(BooleanEnum.TRUE.getCode());
                    toBeDeleted.setMhaId(mha.getId());
                    toBeDeleted.setId(delete.getId());
                    logger.info("[[task=syncMhaTask,mha={},action=Delete]] mysql machine to be logicalDelete :{}",
                            mha.getMhaName(), toBeDeleted);
                    deleteMachines.add(toBeDeleted);
                }
        );
    }
    
    private void getUpdateMachines(List<MachineTbl> machinesInDal,
                                   List<MachineTbl> machinesInMetaDb,
                                   List<MachineTbl> updateMachines,
                                   MhaTbl mha) {
        machinesInDal.forEach(
                remote -> machinesInMetaDb.stream().filter(
                        local -> remote.getIp().equalsIgnoreCase(local.getIp())
                                && remote.getPort().equals(local.getPort())
                ).findFirst().ifPresent(
                        update -> {
                            if (!remote.getMaster().equals(update.getMaster())) {
                                MachineTbl toBeUpdated = new MachineTbl(update.getIp(), update.getPort(), update.getMaster());
                                toBeUpdated.setMaster(remote.getMaster());
                                toBeUpdated.setMhaId(mha.getId());
                                toBeUpdated.setId(update.getId());
                                logger.info("[[task=syncMhaTask,mha={},action=Update]] mysql machine master status change:{}",
                                        mha.getMhaName(),toBeUpdated);
                                updateMachines.add(toBeUpdated);
                            }
                        }
                )
        );
    }
}
