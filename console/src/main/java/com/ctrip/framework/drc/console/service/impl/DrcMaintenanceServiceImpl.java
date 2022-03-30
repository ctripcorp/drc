package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.config.DefaultConsoleConfig;
import com.ctrip.framework.drc.console.dao.entity.*;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.EstablishStatusEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.monitor.DefaultCurrentMetaManager;
import com.ctrip.framework.drc.console.monitor.delay.config.DbClusterSourceProvider;
import com.ctrip.framework.drc.console.monitor.delay.config.MonitorTableSourceProvider;
import com.ctrip.framework.drc.console.service.DrcMaintenanceService;
import com.ctrip.framework.drc.console.service.impl.openapi.OpenService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.console.utils.MySqlUtils;
import com.ctrip.framework.drc.console.vo.MhaGroupPair;
import com.ctrip.framework.drc.core.driver.command.netty.endpoint.DefaultEndPoint;
import com.ctrip.framework.drc.core.driver.command.packet.ResultCode;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.framework.drc.core.monitor.enums.ModuleEnum;
import com.ctrip.framework.drc.core.monitor.reporter.DefaultEventMonitorHolder;
import com.ctrip.platform.dal.dao.DalPojo;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
                machineTbl.setDeleted(BooleanEnum.TRUE.getCode());
                machineTblToBeUpdated.add(machineTbl);
            }
        }
        logger.info("[[mha={}]]slave->master: {}, master->down: {} ", mhaName, masterDb, slaveDb);
        return machineTblToBeUpdated;
    }

    public Boolean updateMhaInstances(MhaInstanceGroupDto dto, boolean isAllMachineInfo) throws Throwable {
        String mhaName = dto.getMhaName();
        MhaTbl mhaTbl = dalUtils.getMhaTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaName.equals(p.getMhaName())).findFirst().orElse(null);
        if (null == mhaTbl) {
            logger.info("mha({}) null", mhaName);
            return false;
        }
        Long mhaId = mhaTbl.getId();
        List<MachineTbl> currentMachineTbls = dalUtils.getMachineTblDao().queryAll().stream().filter(p -> BooleanEnum.FALSE.getCode().equals(p.getDeleted()) && mhaId.equals(p.getMhaId())).collect(Collectors.toList());
        if (checkMasterMachineMatch(dto, currentMachineTbls, mhaId, mhaName)) {
            checkSlaveMachines(dto, currentMachineTbls, mhaId, isAllMachineInfo);
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
            checkSlaveMachines(dto, currentMachineTbls, mhaId, false);
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

    private void checkSlaveMachines(MhaInstanceGroupDto dto, List<MachineTbl> currentMachineTbls, Long mhaId, boolean isAllMachineInfo) throws Throwable {
        List<String> currentMachineIps = currentMachineTbls.stream().map(MachineTbl::getIp).collect(Collectors.toList());
        List<MhaInstanceGroupDto.MySQLInstance> slaves = dto.getSlaves();
        for (MhaInstanceGroupDto.MySQLInstance slave : slaves) {
            String ip = slave.getIp();
            if (!currentMachineIps.contains(ip)) {
                String mhaName = dto.getMhaName();
                int port = slave.getPort();
                insertSlaveMachine(mhaId, mhaName, ip, port, slave.getUuid());
                currentMetaManager.addSlaveMySQL(mhaName, new DefaultEndPoint(ip, port));
            }
        }
        
        if (isAllMachineInfo) {
            // set offline slave machine deleted
            List<String> onlineIps = slaves.stream().map(MhaInstanceGroupDto.MySQLInstance::getIp).collect(Collectors.toList());
            for (MachineTbl machineTbl : currentMachineTbls) {
                if (BooleanEnum.TRUE.getCode().equals(machineTbl.getMaster())) continue;
                String ip = machineTbl.getIp();
                if (!onlineIps.contains(ip)) {
                    String slaveMachineOfflineSyncSwitch = monitorTableSourceProvider.getSlaveMachineOfflineSyncSwitch();
                    if (SWITCH_STATUS_ON.equals(slaveMachineOfflineSyncSwitch)) {
                        logger.info("slaveMachineOfflineSyncSwitch turn on");
                        machineTbl.setDeleted(BooleanEnum.TRUE.getCode());
                        dalUtils.getMachineTblDao().update(machineTbl);
                    }
                    logger.info("slave machine {}:{} offline,already mark as deleted", machineTbl.getIp(), machineTbl.getPort());
                    // because observer maintain only one slaveEndPoint by metaKey, do not to notify remove
                }
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

    public boolean updateMasterReplicator(String mhaName, String newIp) {
        Map<String, ReplicatorTbl> replicators = metaInfoService.getReplicators(mhaName);
        ReplicatorTbl replicatorTbl = replicators.get(newIp);
        if (null != replicatorTbl && replicatorTbl.getMaster().equals(BooleanEnum.FALSE.getCode())) {
            List<ReplicatorTbl> replicatorTblsToBeUpdated = Lists.newArrayList();
            ReplicatorTbl oldMaster = replicators.values().stream().filter(p -> p.getMaster().equals(BooleanEnum.TRUE.getCode())).findFirst().orElse(null);
            replicatorTbl.setMaster(BooleanEnum.TRUE.getCode());
            replicatorTblsToBeUpdated.add(replicatorTbl);
            if (null != oldMaster) {
                oldMaster.setMaster(BooleanEnum.FALSE.getCode());
                replicatorTblsToBeUpdated.add(oldMaster);
            }
            try {
                dalUtils.getReplicatorTblDao().batchUpdate(replicatorTblsToBeUpdated);
                return true;
            } catch (SQLException e) {
                logger.error("Fail update master replicator({}), ", newIp, e);
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
}
