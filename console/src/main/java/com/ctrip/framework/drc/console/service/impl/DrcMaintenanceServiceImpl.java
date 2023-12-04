package com.ctrip.framework.drc.console.service.impl;

import com.ctrip.framework.drc.console.dao.entity.MachineTbl;
import com.ctrip.framework.drc.console.dao.entity.MhaTbl;
import com.ctrip.framework.drc.console.dao.entity.ProxyTbl;
import com.ctrip.framework.drc.console.dao.entity.RouteTbl;
import com.ctrip.framework.drc.console.dto.MhaInstanceGroupDto;
import com.ctrip.framework.drc.console.dto.ProxyDto;
import com.ctrip.framework.drc.console.enums.BooleanEnum;
import com.ctrip.framework.drc.console.enums.TableEnum;
import com.ctrip.framework.drc.console.service.DrcMaintenanceService;
import com.ctrip.framework.drc.console.utils.DalUtils;
import com.ctrip.framework.drc.core.http.ApiResult;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.List;

@Service
@Deprecated
public class DrcMaintenanceServiceImpl implements DrcMaintenanceService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private DalUtils dalUtils = DalUtils.getInstance();

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
